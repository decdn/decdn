//! A pull-through node keeps one warm connection per upstream.
//!
//! A SERVER node with an empty cache pulls two blobs, one after the other, from
//! a PROVIDER node through paid node-to-node pull-through. The server keeps one
//! `cdn/client/v1` and one `cdn/dht/v1` connection per peer, dialled on its
//! main runtime, and every pull leg and DHT call opens a stream on them. So:
//!
//!   1. After a warm-up miss has opened the server's connections to the
//!      provider, two more misses within the idle window open no connection
//!      on the provider beyond their probes. Each miss probes the provider
//!      once on a one-shot `cdn/probe/v1` connection (the positive probe
//!      cache is keyed by hash, and each miss is a new hash); the pull legs,
//!      the header handshakes, the DHT lookups and the republish stores all
//!      ride the warm connections. The count is the provider's
//!      `decdn_iroh_socket_num_conns_opened_total`, which counts inbound and
//!      outbound connections, less its inbound `decdn_probe_requests_total`;
//!      the warm-up miss proves the connection count moves. A single
//!      un-pooled ALPN costs one dial per miss on top of the probes and fails
//!      the check.
//!   2. With those warm connections open, the server stops promptly on SIGTERM
//!      and its router close finishes on its own: every pooled connection's
//!      driver runs on the main runtime, never on a pull leg's throwaway one
//!      (#1675, #2185).
//!
//! Gated behind `anvil-e2e` (off by default). Requires `anvil` + `forge` on
//! `PATH` and a prior build of the `decdn-node` binary:
//!
//! ```bash
//! cargo build -p decdn-node
//! cargo nextest run -p decdn-e2e --features anvil-e2e warm_upstream_connection
//! ```

#![cfg(feature = "anvil-e2e")]
// Test scaffolding legitimately uses unwrap/expect/panic; the workspace
// anti-panic policy targets runtime code.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::time::Duration;

use alloy::primitives::U256;
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::client::ClientFixture;
use decdn_e2e::node::NodeFixture;

/// Overall ceiling: two daemons plus a chain need the full standard tier.
const OVERALL_TIMEOUT: Duration = Duration::from_secs(300);

/// Bytes per bao chunk group (16 KiB).
const CHUNK_GROUP: usize = 16 * 1024;

/// USDC (base units) minted to the server operator so its buyer pool to the
/// provider never starves.
const SERVER_BUYER_USDC: u64 = 1_000_000_000;

/// How long the server gets to exit after SIGTERM. A connection whose driver
/// died with a pull leg's runtime parks the router close for good, so any
/// bound catches it; this one leaves room for the final redeem sweep against
/// anvil and the store flushes on a loaded runner.
const SERVER_STOP_BUDGET: Duration = Duration::from_secs(30);

/// Deterministic pseudo-random blob, distinct per `seed`.
fn make_blob(len: usize, seed: u32) -> Vec<u8> {
    let mut v = vec![0u8; len];
    let mut x: u32 = 0x9E37_79B9 ^ seed;
    for b in &mut v {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes().first().copied().unwrap_or(0);
    }
    v
}

#[tokio::test(flavor = "multi_thread")]
async fn two_misses_share_one_upstream_connection() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("warm upstream connection e2e exceeded the overall timeout")??;
    Ok(())
}

async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;

    // Every blob lives only on the PROVIDER, so every server fetch is a miss
    // that only a paid pull from the provider can fill.
    let warm = make_blob(3 * CHUNK_GROUP + 7, 1);
    let blob_a = make_blob(16 * CHUNK_GROUP + 11, 2);
    let blob_b = make_blob(16 * CHUNK_GROUP + 13, 3);
    let (provider, hashes) =
        NodeFixture::launch_with_blobs(&chain, "US", &[&warm, &blob_a, &blob_b]).await?;
    let mut hashes = hashes.into_iter();
    let warm_hash = hashes.next().context("provider missing warm hash")?;
    let hash_a = hashes.next().context("provider missing blob A hash")?;
    let hash_b = hashes.next().context("provider missing blob B hash")?;

    // Seat the provider as an authorized origin for a fresh namespace, so the
    // server's directory fallback resolves it on a miss.
    let publisher = PrivateKeySigner::random();
    chain.vet_publisher(publisher.address()).await?;
    let namespace = chain.create_namespace(&publisher).await?;
    chain
        .add_origin(&publisher, namespace, provider.operator_addr())
        .await?;

    let server = NodeFixture::launch_pull_through_cache(&chain, "US", &[&provider]).await?;
    chain
        .fund_node_as_buyer(server.operator_addr(), U256::from(SERVER_BUYER_USDC))
        .await?;
    let client = ClientFixture::new(&chain).await?;

    // Warm-up: the session's first delivery is itself a miss, which opens the
    // server's connections to the provider.
    let conns_before_warm_up = provider
        .scrape_metric("decdn_iroh_socket_num_conns_opened_total")
        .await?;
    let (mut session, _) = client
        .open_session_in_namespace(&chain, &server, warm_hash, namespace)
        .await
        .context("client session set-up")?;
    let conns_after_warm_up = provider
        .scrape_metric("decdn_iroh_socket_num_conns_opened_total")
        .await?;
    anyhow::ensure!(
        conns_after_warm_up > conns_before_warm_up,
        "the warm-up miss must open the server's connections to the provider \
         ({conns_before_warm_up} -> {conns_after_warm_up}); otherwise the counter \
         does not see inbound connections and the check below proves nothing"
    );
    let pulls_before = server
        .scrape_metric("decdn_node_pull_success_total")
        .await?;
    let probes_before = provider.scrape_metric("decdn_probe_requests_total").await?;

    // (1) Two more misses in a row, inside the idle window.
    for (hash, blob) in [(hash_a, &blob_a), (hash_b, &blob_b)] {
        let bytes = client
            .fetch_once(&mut session, hash, 0, namespace)
            .await
            .context("miss through the server")?;
        anyhow::ensure!(&bytes == blob, "the miss must deliver the blob byte-exact");
    }
    let pulls_after = server
        .scrape_metric("decdn_node_pull_success_total")
        .await?;
    anyhow::ensure!(
        pulls_after == pulls_before + 2,
        "both fetches must be paid upstream pulls ({pulls_before} -> {pulls_after})"
    );
    let conns_after_misses = provider
        .scrape_metric("decdn_iroh_socket_num_conns_opened_total")
        .await?;
    let probes_after = provider.scrape_metric("decdn_probe_requests_total").await?;
    let new_conns = conns_after_misses.saturating_sub(conns_after_warm_up);
    let probes = probes_after.saturating_sub(probes_before);
    anyhow::ensure!(
        probes <= 2,
        "each miss probes the provider at most once, got {probes} probes"
    );
    anyhow::ensure!(
        new_conns <= probes,
        "two misses on warm connections must open no connection on the provider \
         beyond their one-shot probes: {new_conns} new connections for {probes} probes"
    );

    // (2) Shutdown with the warm connections still open stays prompt.
    let (status, took) = server.terminate(SERVER_STOP_BUDGET).await?;
    anyhow::ensure!(
        status.success(),
        "the server must exit successfully on SIGTERM, got {status} after {took:?}"
    );
    let gave_up = server.log_line(&["router shutdown"]);
    anyhow::ensure!(
        gave_up.is_none(),
        "the server's router close must finish on its own with warm connections open, \
         got: {gave_up:?}"
    );
    Ok(())
}
