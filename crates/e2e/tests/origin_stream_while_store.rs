//! Serve-side stream-while-store e2e (#1130).
//!
//! A whole-blob cache miss whose origin publishes a `{H}.obao4` pre-order
//! outboard is served by streaming straight from the origin into the paying
//! client while teeing the bytes into the local store
//! (`serve_via_local_outboard`, tried in `dispatch.rs` before the buffered
//! `try_local_populate` fallback). Three journeys prove the whole tier
//! end to end, against a real anvil chain, a real `decdn-node` daemon, and a
//! real paid `cdn/client/v1` fetch:
//!
//! 1. **Streams (outboard present).** The stream-while-store tier fires
//!    (`decdn_local_outboard_serves_total` goes 0 → 1), the client gets the
//!    exact bytes on the first attempt, and the node ends up holding the blob
//!    in its cache (teed) — proven by a second fetch that no longer needs the
//!    origin at all.
//! 2. **Falls back (no outboard).** Without a published outboard the node
//!    cannot open the local-outboard pull (`open_local_outboard_pull` returns
//!    `Ok(None)`), so the request degrades to the buffered `populate_local`
//!    fallback: the fetch still succeeds and is byte-correct, but the
//!    stream-path counter never moves.
//! 3. **Corrupt-safe.** A tampered `{H}.obao4` (a flipped byte in the middle,
//!    so the outboard's length is unchanged and the tamper is only caught by
//!    bao verification, not a length pre-check) must fail the client's fetch
//!    without panicking the daemon or leaving it unhealthy — post-#1512 this
//!    path carries no slash consequence, only a client-side rejection and node
//!    liveness matter.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH` and a prior build of the `decdn-node` binary:
//!
//! ```bash
//! cargo build -p decdn-node
//! cargo nextest run -p decdn-e2e --features anvil-e2e origin_stream_while_store
//! ```

#![cfg(feature = "anvil-e2e")]
// Test scaffolding legitimately uses unwrap/expect/panic; the workspace
// anti-panic policy targets runtime code.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::duration_suboptimal_units
)]

use std::time::Duration;

use alloy::primitives::U256;
use anyhow::Context;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::client::ClientFixture;
use decdn_e2e::node::NodeFixture;

/// Overall ceiling so an unbounded await fails fast with a clear message.
const OVERALL_TIMEOUT: Duration = Duration::from_secs(300);

/// Bytes per bao chunk group (matches `decdn_bao_range::IROH_BLOCK_SIZE`,
/// chunk-log 4 == 16 KiB chunk groups). A multi-chunk-group blob is required so
/// the streaming path is real rather than a single-group edge case.
const CHUNK_GROUP: usize = 16 * 1024;

/// Deterministic pseudo-random blob spanning several chunk groups plus a
/// ragged final group, so the journey exercises both interior groups and the
/// right edge.
fn make_blob(len: usize) -> Vec<u8> {
    let mut v = vec![0u8; len];
    let mut x: u32 = 0x9E37_79B9;
    for b in &mut v {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes().first().copied().unwrap_or(0);
    }
    v
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_while_store_serves_on_first_fetch_and_tees_into_cache() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run_streams()))
        .await
        .context("stream-while-store e2e exceeded the overall timeout")??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_while_store_falls_back_to_buffered_populate_without_an_outboard()
-> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run_no_outboard_fallback()))
        .await
        .context("no-outboard fallback e2e exceeded the overall timeout")??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_while_store_rejects_a_corrupt_outboard_without_destabilizing_the_node()
-> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run_corrupt_outboard()))
        .await
        .context("corrupt-outboard e2e exceeded the overall timeout")??;
    Ok(())
}

/// Test A — the outboard is present: the stream-while-store tier must fire,
/// deliver byte-correct content on the FIRST attempt, and tee the blob into
/// the node's own cache store.
async fn run_streams() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;
    let node = NodeFixture::launch_pull_through_cache(&chain, "US", &[]).await?;

    // A few hundred KB, multi-chunk-group blob so the streaming path is real.
    let blob = make_blob(20 * CHUNK_GROUP + 777);
    let hash = node.seed_origin_blob_with_outboard(&blob)?;

    let client = ClientFixture::new(&chain).await?;

    let before = node
        .scrape_metric("decdn_local_outboard_serves_total")
        .await?;
    let outcome = client.fetch(&chain, &node, hash, U256::ZERO).await?;
    anyhow::ensure!(
        outcome.bytes == blob,
        "delivered bytes must hash-match the seeded blob: got {} bytes, expected {}",
        outcome.bytes.len(),
        blob.len()
    );
    let after = node
        .scrape_metric("decdn_local_outboard_serves_total")
        .await?;
    anyhow::ensure!(
        after == before + 1,
        "the stream-while-store tier must fire exactly once on a first fetch with a published \
         outboard: before={before}, after={after}"
    );

    // Prove the tee landed: a second fetch (by a fresh, differently-funded
    // client, so nothing about the first buyer's channel/voucher state can
    // mask the answer) must still succeed and be byte-correct, and must NOT
    // bump the stream-while-store counter again — the blob is now served from
    // the node's own cache store, not re-streamed from origin.
    let second_client = ClientFixture::new(&chain).await?;
    let before_second = node
        .scrape_metric("decdn_local_outboard_serves_total")
        .await?;
    let second = second_client.fetch(&chain, &node, hash, U256::ZERO).await?;
    anyhow::ensure!(
        second.bytes == blob,
        "a second fetch (now served from the teed cache entry) must still be byte-correct"
    );
    let after_second = node
        .scrape_metric("decdn_local_outboard_serves_total")
        .await?;
    anyhow::ensure!(
        after_second == before_second,
        "a fetch against an already-cached blob must NOT re-enter the stream-while-store tier: \
         before={before_second}, after={after_second}"
    );

    Ok(())
}

/// Test B — no outboard is published: `open_local_outboard_pull` declines
/// (`Ok(None)`), so the request must degrade to the buffered
/// `try_local_populate` fallback. The fetch still succeeds and is
/// byte-correct, but the stream-path counter must stay at zero.
async fn run_no_outboard_fallback() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;
    let node = NodeFixture::launch_pull_through_cache(&chain, "US", &[]).await?;

    let blob = make_blob(20 * CHUNK_GROUP + 321);
    // No outboard: `seed_origin_blob` writes only `{H}`, not the sibling
    // `{H}.obao4`.
    let hash = node.seed_origin_blob(&blob)?;

    let client = ClientFixture::new(&chain).await?;

    let outcome = client.fetch(&chain, &node, hash, U256::ZERO).await?;
    anyhow::ensure!(
        outcome.bytes == blob,
        "the buffered populate_local fallback must still deliver byte-correct content: got {} \
         bytes, expected {}",
        outcome.bytes.len(),
        blob.len()
    );

    let stream_serves = node
        .scrape_metric("decdn_local_outboard_serves_total")
        .await?;
    anyhow::ensure!(
        stream_serves == 0,
        "the stream-while-store tier must NOT fire when the origin publishes no outboard: \
         decdn_local_outboard_serves_total = {stream_serves}"
    );

    Ok(())
}

/// Test C — the published outboard is corrupt (a single byte flipped in the
/// middle, so its length is unchanged and the tamper is caught by bao
/// verification rather than a cheap length check). The client's fetch must
/// fail; the daemon must neither panic nor become unhealthy afterward. No
/// slash surface is touched post-#1512 — this is a pure client-side rejection
/// plus a node-liveness assertion.
async fn run_corrupt_outboard() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;
    let node = NodeFixture::launch_pull_through_cache(&chain, "US", &[]).await?;

    let blob = make_blob(20 * CHUNK_GROUP + 999);
    let hash = node.seed_origin_blob_with_outboard(&blob)?;

    // Corrupt the sibling `{H}.obao4` in place: flip one byte in the middle so
    // the file's length is unchanged. `{root}/{hex[..2]}/{hex}.obao4` mirrors
    // the fs-origin shard layout `NodeFixture::seed_origin_blob_with_outboard`
    // writes into.
    let hex = hash.to_hex();
    let shard = hex.as_str().get(..2).context("blob hex too short")?;
    let outboard_path = node
        .origin_root()
        .join(shard)
        .join(format!("{}.obao4", hex.as_str()));
    let mut outboard_bytes =
        std::fs::read(&outboard_path).context("read seeded outboard before corrupting it")?;
    anyhow::ensure!(
        !outboard_bytes.is_empty(),
        "seeded outboard must be non-empty to corrupt its middle byte"
    );
    let mid = outboard_bytes.len() / 2;
    if let Some(byte) = outboard_bytes.get_mut(mid) {
        *byte ^= 0xFF;
    }
    std::fs::write(&outboard_path, &outboard_bytes).context("write corrupted outboard")?;

    let client = ClientFixture::new(&chain).await?;

    let result = client.fetch(&chain, &node, hash, U256::ZERO).await;
    anyhow::ensure!(
        result.is_err(),
        "a fetch against a corrupt outboard must fail, not silently deliver bytes"
    );

    // The daemon must not have panicked and must still answer its admin RPC.
    node.wait_healthy(Duration::from_secs(30))
        .await
        .context("node must stay healthy after serving a corrupt outboard")?;

    // ADR 037 AC 13 / ADR 038 AC 4: the bytes teed before the tamper was caught
    // must NOT have been committed as a local copy. Prove it the way test A
    // proves the opposite — by taking the origin away and asking again. Delete
    // both the data object and the corrupt outboard, so a second fetch has no
    // origin left to stream from; it can only succeed if the node kept a copy
    // of the partial, unverified bytes. A fresh client keeps the first buyer's
    // channel state from masking the answer.
    let data_path = node.origin_root().join(shard).join(hex.as_str());
    std::fs::remove_file(&data_path).context("remove seeded origin data object")?;
    std::fs::remove_file(&outboard_path).context("remove corrupted origin outboard")?;

    let second_client = ClientFixture::new(&chain).await?;
    let after_origin_removed = second_client.fetch(&chain, &node, hash, U256::ZERO).await;
    anyhow::ensure!(
        after_origin_removed.is_err(),
        "a fetch that failed bao verification must leave NO committed local copy, so a second \
         fetch with the origin removed must fail — it succeeded, meaning partial unverified \
         bytes were committed"
    );

    Ok(())
}
