//! e2e proof that a paid fetch against an fs-origin blob whose tree already
//! carries a `.obao4` sibling outboard is served zero-copy (#1511): the node
//! never imports the bytes into its own cache store.
//!
//! A real anvil chain, a real `decdn-node` daemon, and a real paid
//! `cdn/client/v1` client drive two fetches against one node whose origin
//! backend holds a blob it has never cached — a whole-blob fetch (the
//! `byte_offset == 0, byte_len == 0` shape) and a ranged fetch from a
//! chunk-group-aligned offset to the end of the blob (`byte_offset > 0`, the
//! shape that reaches the origin range tier, ADR 038 §Serve side). Both must
//! deliver byte-correct content, and — the point of the journey — the node's
//! own blob store must never come to hold the hash: proof the zero-copy path
//! (Tasks 1-6, `ServeSource::OriginZeroCopy`) served straight from the
//! operator's file + outboard with no `iroh-blobs` import.
//!
//! Store emptiness is checked with [`decdn_e2e::node::NodeFixture::store_blob_present`]
//! (added by this task): the daemon is killed and its `cache_dir` is reopened
//! directly through `CacheEngine::has`, which is the store-inspection
//! accessor the fixture module exposes (no `store_blob_count`/`has`-style
//! method existed before this journey). That is the LAST step of the
//! journey — nothing can be fetched from this node afterward.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH` and a prior build of the `decdn-node` binary:
//!
//! ```bash
//! cargo build -p decdn-node
//! cargo nextest run -p decdn-e2e --features anvil-e2e fs_origin_zero_copy
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

/// Standard journey tier (see [`decdn_e2e::timeout`] for the tier rule): two
/// paid fetches plus a kill-and-reopen, well under the 94s p100 the tier is
/// sized against.
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::STANDARD;

/// Bytes per bao chunk group (matches `decdn_bao_range::IROH_BLOCK_SIZE`,
/// chunk-log 4 == 16 KiB chunk groups), mirroring `origin_stream_while_store`.
const CHUNK_GROUP: usize = 16 * 1024;

/// Deterministic pseudo-random payload spanning several chunk groups plus a
/// ragged final group, mirroring `origin_stream_while_store::make_blob` so
/// this journey's payload shape is comparable.
fn make_payload(len: usize) -> Vec<u8> {
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
async fn fs_origin_zero_copy_paid_fetch_leaves_store_empty() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("fs-origin zero-copy e2e exceeded the overall timeout")??;
    Ok(())
}

async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;
    // Empty cache node: nothing is warmed into the store at launch, so any
    // presence found afterward can only have come from serving this journey's
    // fetches.
    let node = NodeFixture::launch_pull_through_cache(&chain, "US", &[]).await?;

    // A few chunk groups plus a ragged tail, so both the whole-blob and the
    // ranged fetch below cross real chunk-group boundaries.
    let payload = make_payload(5 * CHUNK_GROUP + 321);
    // Seeds the origin backend with BOTH the data object and its sibling
    // `.obao4` pre-order outboard (`seed_origin_blob` alone would leave the
    // range tier unreachable, #1372) — and touches only the origin dir, never
    // the cache store.
    let hash = node.seed_origin_blob_with_outboard(&payload)?;

    let client = ClientFixture::new(&chain).await?;

    // Whole-blob fetch (`byte_offset == 0, byte_len == 0`). `open_session`
    // also serves as the pool's readiness gate: a session is handed back only
    // once this warm-up has actually been served, so there is no ambiguity
    // between "not ready yet" and "the zero-copy path is broken".
    let (mut session, whole) = client.open_session(&chain, &node, hash).await?;
    anyhow::ensure!(
        whole == payload,
        "whole-blob fetch must deliver the exact seeded bytes: got {} bytes, expected {}",
        whole.len(),
        payload.len()
    );

    // Ranged fetch from a chunk-group-aligned offset to the end of the blob
    // (`byte_offset > 0`), the shape that reaches the origin range tier
    // (`export_bao_range_stream_from_origin`, ADR 038 §Serve side). The
    // client-pull surface this fixture exposes has no bounded `byte_len`
    // parameter (only `stream_fetch_tracked`'s `byte_offset`, which always
    // requests the tail); a bounded `[off, off+len)` fetch would need driving
    // the lower-level `open_progressive_pull` + raw bao-decode directly, which
    // is a materially bigger lift than this journey needs to prove the range
    // tier is reached and stays zero-copy. A group-aligned tail from a
    // non-zero offset already exercises that tier.
    let off = CHUNK_GROUP as u64;
    let ranged = client
        .fetch_once(&mut session, hash, off, U256::ZERO)
        .await?;
    let expected_tail = &payload[usize::try_from(off).context("offset fits in usize")?..];
    anyhow::ensure!(
        ranged == expected_tail,
        "ranged fetch must deliver the exact tail from offset {off}: got {} bytes, expected {}",
        ranged.len(),
        expected_tail.len()
    );

    // The point of the journey: the node's own blob store must never have
    // come to hold this hash. `store_blob_present` kills the daemon and
    // reopens its `cache_dir` directly through `CacheEngine::has` — the
    // store-inspection accessor `decdn_e2e::node` exposes (no
    // `store_blob_count`/`has`-style method existed before this task). This
    // is deliberately the LAST assertion: nothing can be fetched from this
    // node afterward.
    let present = node.store_blob_present(hash).await?;
    anyhow::ensure!(
        !present,
        "zero-copy must not import into the store: node's cache reports hash {} present after \
         two served paid fetches",
        hash.to_hex()
    );

    Ok(())
}
