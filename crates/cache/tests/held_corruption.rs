//! A held blob whose stored bytes change after admission (#1984).
//!
//! Every serve export validates the exported chunk groups against the content
//! root. A mismatch or short read over held content must quarantine the hash:
//! the engine stops serving and announcing it, releases the entry to GC, and
//! lifts the quarantine once the sweep reclaims it, so a later pull-through
//! admits a verified copy.

use std::collections::HashSet;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use bao_tree::io::outboard::PreOrderMemOutboard;
use decdn_cache::range_pull::{IROH_BLOCK_SIZE, align_range, encode_verified_range};
use decdn_cache::{
    CacheEngine, CacheMetrics, Hash, HttpOrigin, Origin, PinnedHashes, RetryPolicy, ServeAudit,
};
use futures_util::StreamExt;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

mod util;

/// Large enough that the store keeps the data in `data/{hex}.data` rather than
/// inline in its database (the inline threshold is 16 KiB).
const BLOB_LEN: usize = 200 * 1024 + 1234;

/// Large enough that the outboard (64 bytes per internal node) also passes the
/// 16 KiB inline threshold and lands in `data/{hex}.obao4`.
const LARGE_BLOB_LEN: usize = 4 * 1024 * 1024 + 512 * 1024;

const GC_INTERVAL: Duration = Duration::from_millis(200);

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

fn pin(hash: Hash) -> PinnedHashes {
    PinnedHashes::new(HashSet::from([decdn_config_types::Hash::from_bytes(
        *hash.as_bytes(),
    )]))
}

fn store_file(dir: &Path, hash: Hash, ext: &str) -> anyhow::Result<PathBuf> {
    let file = dir.join("data").join(format!("{}.{ext}", hash.to_hex()));
    anyhow::ensure!(
        file.exists(),
        "store layout changed: {} is missing",
        file.display()
    );
    Ok(file)
}

/// Flip one byte of a stored file of `hash`, behind the live store.
fn flip(dir: &Path, hash: Hash, ext: &str, offset: u64) -> anyhow::Result<()> {
    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(store_file(dir, hash, ext)?)?;
    f.seek(SeekFrom::Start(offset))?;
    let mut byte = [0u8; 1];
    f.read_exact(&mut byte)?;
    byte[0] ^= 0xff;
    f.seek(SeekFrom::Start(offset))?;
    f.write_all(&byte)?;
    f.sync_all()?;
    Ok(())
}

fn tamper(dir: &Path, hash: Hash) -> anyhow::Result<()> {
    flip(dir, hash, "data", 100 * 1024)
}

/// Drain a serve export of `[offset, offset + len)`. Returns the error text of
/// the terminal `Err` item, or `None` when the export completed.
async fn serve_error(
    engine: &CacheEngine,
    hash: Hash,
    offset: u64,
    len: u64,
    blob_len: usize,
) -> anyhow::Result<Option<String>> {
    let blob_size = u64::try_from(blob_len)?;
    let mut stream = engine
        .export_bao_range_stream(hash, offset, len, blob_size)
        .await?;
    while let Some(item) = stream.next().await {
        if let Err(err) = item {
            let mut text = err.to_string();
            let mut source = std::error::Error::source(&err);
            while let Some(cause) = source {
                text.push_str(": ");
                text.push_str(&cause.to_string());
                source = cause.source();
            }
            return Ok(Some(text));
        }
    }
    Ok(None)
}

async fn serve_fails(engine: &CacheEngine, hash: Hash) -> anyhow::Result<bool> {
    Ok(serve_error(engine, hash, 0, 0, BLOB_LEN).await?.is_some())
}

/// Wait until GC has reclaimed `hash`, observed through `inspect`, which never
/// lifts a quarantine.
async fn await_reclaim(engine: &CacheEngine, hash: Hash) -> anyhow::Result<()> {
    let deadline = std::time::Instant::now() + GC_INTERVAL * 64;
    while std::time::Instant::now() < deadline {
        tokio::time::sleep(GC_INTERVAL).await;
        if engine.inspect(hash).await?.size_bytes.is_none() {
            return Ok(());
        }
    }
    anyhow::bail!("GC did not reclaim the quarantined entry")
}

#[tokio::test]
async fn a_serve_that_trips_stored_corruption_quarantines_the_hash() -> anyhow::Result<()> {
    let payload = util::make_blob(BLOB_LEN);
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
    let err = serve_error(&engine, hash, 0, 0, BLOB_LEN).await?;
    anyhow::ensure!(
        err.as_deref()
            .is_some_and(|e| e.to_lowercase().contains("leaf")),
        "the export must fail leaf validation on the tampered group; got {err:?}"
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
        engine.serve_audit(hash).await? == ServeAudit::Unavailable { withdrawn: true },
        "the delivery gate reports a quarantined hash as withdrawn, so dispatch never fills it"
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
    let payload = util::make_blob(BLOB_LEN);
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
async fn a_tampered_outboard_quarantines_on_a_parent_mismatch() -> anyhow::Result<()> {
    let payload = util::make_blob(LARGE_BLOB_LEN);
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

    // The first pre-order pair is the root's children, so every export checks it.
    flip(tmp.path(), hash, "obao4", 0)?;
    let err = serve_error(&engine, hash, 0, 0, LARGE_BLOB_LEN).await?;
    anyhow::ensure!(
        err.as_deref()
            .is_some_and(|e| e.to_lowercase().contains("parent")),
        "the export must fail parent validation on the tampered outboard; got {err:?}"
    );
    anyhow::ensure!(
        engine.is_quarantined(hash),
        "a parent mismatch quarantines the hash"
    );
    anyhow::ensure!(metrics.held_corruption_quarantined.get() == 1);
    Ok(())
}

#[tokio::test]
async fn a_truncated_data_file_quarantines_the_hash() -> anyhow::Result<()> {
    let payload = util::make_blob(BLOB_LEN);
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

    let file =
        std::fs::OpenOptions::new()
            .write(true)
            .open(store_file(tmp.path(), hash, "data")?)?;
    file.set_len(64 * 1024)?;
    file.sync_all()?;

    anyhow::ensure!(
        serve_fails(&engine, hash).await?,
        "the export must fail on the short read"
    );
    anyhow::ensure!(
        engine.is_quarantined(hash),
        "a complete blob whose data file is short is corrupt"
    );
    Ok(())
}

#[tokio::test]
async fn an_absent_blob_export_fault_does_not_quarantine() -> anyhow::Result<()> {
    let payload = util::make_blob(BLOB_LEN);
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
    anyhow::ensure!(
        serve_error(&engine, hash, 0, 0, BLOB_LEN).await?.is_some(),
        "exporting an absent blob must end in an error item"
    );
    anyhow::ensure!(
        !engine.is_quarantined(hash),
        "an absent blob is not stored corruption"
    );
    anyhow::ensure!(metrics.held_corruption_quarantined.get() == 0);
    Ok(())
}

/// A partial blob's data file is sparse, so exporting an absent range reads
/// zeros that fail validation like corruption. That must not withdraw a healthy
/// partial. Corruption inside a present range still quarantines it.
#[tokio::test]
async fn a_partial_blob_quarantines_only_for_corruption_in_a_present_range() -> anyhow::Result<()> {
    let payload = util::make_blob(BLOB_LEN);
    let blob_size = u64::try_from(payload.len())?;
    let ob = PreOrderMemOutboard::create(&payload, IROH_BLOCK_SIZE);
    let root: [u8; 32] = *ob.root.as_bytes();
    let hash = Hash::from(root);
    let first_group = align_range(0, 16 * 1024, blob_size)?;
    let bao = encode_verified_range(
        root,
        &first_group,
        payload.get(..16 * 1024).unwrap_or_default(),
        bytes::Bytes::from(ob.data),
    )?;

    let tmp = tempfile::tempdir()?;
    let metrics = Arc::new(CacheMetrics::default());
    let engine = open(
        tmp.path(),
        "http://127.0.0.1:9",
        PinnedHashes::empty(),
        &metrics,
        Duration::ZERO,
    )
    .await?;
    engine
        .admit_bao(hash, first_group.chunk_ranges().clone(), bao)
        .await?;
    anyhow::ensure!(!engine.present_ranges(hash).await?.is_complete());

    let absent = serve_error(&engine, hash, 64 * 1024, 16 * 1024, BLOB_LEN).await?;
    anyhow::ensure!(
        absent.is_some(),
        "exporting an absent range of a partial must fail"
    );
    anyhow::ensure!(
        !engine.is_quarantined(hash),
        "an absent range is not corruption; got {absent:?}"
    );
    anyhow::ensure!(
        serve_error(&engine, hash, 0, 16 * 1024, BLOB_LEN)
            .await?
            .is_none(),
        "the present range still serves"
    );

    flip(tmp.path(), hash, "data", 100)?;
    anyhow::ensure!(
        serve_error(&engine, hash, 0, 16 * 1024, BLOB_LEN)
            .await?
            .is_some(),
        "the tampered present range must fail validation"
    );
    anyhow::ensure!(
        engine.is_quarantined(hash),
        "corruption inside a present range quarantines the partial"
    );
    Ok(())
}

/// Admit the first `prefix_len` bytes of `payload` into a fresh engine as a
/// partial blob. Returns the engine, its directory, the hash, and the outboard
/// tree geometry.
async fn partial_prefix(
    payload: &[u8],
    prefix_len: u64,
    metrics: &Arc<CacheMetrics>,
) -> anyhow::Result<(CacheEngine, tempfile::TempDir, Hash, bao_tree::BaoTree)> {
    let blob_size = u64::try_from(payload.len())?;
    let ob = PreOrderMemOutboard::create(payload, IROH_BLOCK_SIZE);
    let root: [u8; 32] = *ob.root.as_bytes();
    let hash = Hash::from(root);
    let prefix = align_range(0, prefix_len, blob_size)?;
    let bao = encode_verified_range(
        root,
        &prefix,
        payload
            .get(..usize::try_from(prefix_len)?)
            .unwrap_or_default(),
        bytes::Bytes::from(ob.data),
    )?;
    let tmp = tempfile::tempdir()?;
    let engine = open(
        tmp.path(),
        "http://127.0.0.1:9",
        PinnedHashes::empty(),
        metrics,
        Duration::ZERO,
    )
    .await?;
    engine
        .admit_bao(hash, prefix.chunk_ranges().clone(), bao)
        .await?;
    anyhow::ensure!(!engine.present_ranges(hash).await?.is_complete());
    Ok((engine, tmp, hash, ob.tree))
}

/// A parent mismatch names a tree node whose range is in 1 KiB chunks. A node
/// that covers only present data must quarantine the partial.
#[tokio::test]
async fn a_parent_mismatch_inside_a_partial_blobs_present_ranges_quarantines() -> anyhow::Result<()>
{
    use bao_tree::ChunkRanges;
    use bao_tree::iter::BaoChunk;

    let payload = util::make_blob(LARGE_BLOB_LEN);
    let prefix_len: u64 = 1024 * 1024;
    let metrics = Arc::new(CacheMetrics::default());
    let (engine, tmp, hash, tree) = partial_prefix(&payload, prefix_len, &metrics).await?;

    // The largest non-root parent that lies wholly inside the admitted prefix.
    let prefix_ranges = ChunkRanges::from(..bao_tree::ChunkNum(prefix_len / 1024));
    let node = tree
        .ranges_pre_order_chunks_iter_ref(&prefix_ranges, 0)
        .find_map(|chunk| match chunk {
            BaoChunk::Parent { node, is_root, .. }
                if !is_root
                    && (ChunkRanges::from(node.chunk_range()) - &prefix_ranges).is_empty() =>
            {
                Some(node)
            }
            _ => None,
        })
        .ok_or_else(|| anyhow::anyhow!("no parent node inside the prefix"))?;
    let offset = tree
        .pre_order_offset(node)
        .ok_or_else(|| anyhow::anyhow!("the node has no outboard slot"))?;
    flip(tmp.path(), hash, "obao4", offset * 64)?;

    let err = serve_error(&engine, hash, 0, prefix_len, LARGE_BLOB_LEN).await?;
    anyhow::ensure!(
        err.as_deref()
            .is_some_and(|e| e.to_lowercase().contains("parent")),
        "the export must fail parent validation; got {err:?}"
    );
    anyhow::ensure!(
        engine.is_quarantined(hash),
        "a parent mismatch inside the present ranges quarantines the partial"
    );
    Ok(())
}

/// A short read names no location, so it counts when the whole requested range
/// is present.
#[tokio::test]
async fn a_short_read_over_a_partial_blobs_present_range_quarantines() -> anyhow::Result<()> {
    let payload = util::make_blob(BLOB_LEN);
    let metrics = Arc::new(CacheMetrics::default());
    let (engine, tmp, hash, _) = partial_prefix(&payload, 64 * 1024, &metrics).await?;

    let file =
        std::fs::OpenOptions::new()
            .write(true)
            .open(store_file(tmp.path(), hash, "data")?)?;
    file.set_len(20 * 1024)?;
    file.sync_all()?;

    anyhow::ensure!(
        serve_error(&engine, hash, 0, 64 * 1024, BLOB_LEN)
            .await?
            .is_some(),
        "the export must fail on the short read"
    );
    anyhow::ensure!(
        engine.is_quarantined(hash),
        "a short read inside the present ranges quarantines the partial"
    );
    Ok(())
}

/// GC reclaims the quarantined entry, the quarantine lifts, and a pull-through
/// admits a verified copy that serves. Pinned, so the test also proves the
/// quarantine releases a pinned hash to GC.
#[tokio::test]
async fn gc_reclaim_lifts_the_quarantine_and_a_pinned_hash_re_admits() -> anyhow::Result<()> {
    let payload = util::make_blob(BLOB_LEN);
    let (server, hash) = serve(&payload).await;
    let tmp = tempfile::tempdir()?;
    let metrics = Arc::new(CacheMetrics::default());
    let engine = open(tmp.path(), &server.uri(), pin(hash), &metrics, GC_INTERVAL).await?;
    engine.get(hash).await?;

    tamper(tmp.path(), hash)?;
    anyhow::ensure!(
        serve_fails(&engine, hash).await?,
        "the tampered serve fails"
    );
    anyhow::ensure!(engine.is_quarantined(hash));

    await_reclaim(&engine, hash).await?;
    anyhow::ensure!(
        engine.is_quarantined(hash),
        "inspect observes the reclaim without lifting"
    );
    // `serve_audit` lifts a reclaimed quarantine, then reports a plain miss.
    anyhow::ensure!(
        engine.serve_audit(hash).await? == ServeAudit::Unavailable { withdrawn: false },
        "a reclaimed hash is a plain miss that dispatch may fill"
    );
    anyhow::ensure!(
        !engine.is_quarantined(hash),
        "serve_audit lifts the quarantine"
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
    anyhow::ensure!(
        matches!(
            engine.serve_audit(hash).await?,
            ServeAudit::Serveable { .. }
        ),
        "the re-admitted copy is serveable"
    );
    Ok(())
}

/// The origin rescan lifts a reclaimed quarantine with no request touching
/// the hash, so a reclaimed hash can rejoin the announce set on its own.
#[tokio::test]
async fn an_origin_rescan_lifts_a_reclaimed_quarantine() -> anyhow::Result<()> {
    let payload = util::make_blob(BLOB_LEN);
    let (server, hash) = serve(&payload).await;
    let tmp = tempfile::tempdir()?;
    let metrics = Arc::new(CacheMetrics::default());
    let engine = open(tmp.path(), &server.uri(), pin(hash), &metrics, GC_INTERVAL).await?;
    engine.get(hash).await?;

    tamper(tmp.path(), hash)?;
    anyhow::ensure!(serve_fails(&engine, hash).await?);
    anyhow::ensure!(engine.is_quarantined(hash));

    await_reclaim(&engine, hash).await?;
    anyhow::ensure!(engine.is_quarantined(hash), "nothing has lifted it yet");
    engine.rescan_origins().await;
    anyhow::ensure!(
        !engine.is_quarantined(hash),
        "the rescan lifts a quarantine whose entry GC reclaimed"
    );
    Ok(())
}
