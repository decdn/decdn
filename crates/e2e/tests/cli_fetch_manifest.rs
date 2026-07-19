//! Live anvil-backed e2e for the CLI `decdn fetch` **`DECDNMAN` file-manifest
//! reconstruction** path (issue #1183). The companion to `cli_fetch_topup.rs`
//! (which drives the buyer top-up kernel): this one drives the shipped `decdn`
//! **binary** end to end — `decdn fetch <manifest-hash>` — against a real node
//! serving a manifest blob plus its chunk blobs over `cdn/client/v1`.
//!
//! Why the whole binary rather than a `reconstruct()` unit test: the load-bearing
//! invariant of the feature is that `fetch` rebuilds the `ChannelContext` **per
//! chunk** (`crates/cli/src/commands/fetch.rs`, `ChunkFetcher::fetch`), because a
//! chunked file issues many sequential paid pulls on one channel and the context
//! snapshots the voucher watermark (`prior_nonce`). A hoisted context would
//! re-sign an already-spent nonce on the second chunk, the node would reject the
//! replay, and reconstruction would fail. A `reconstruct()` test with an
//! in-memory fake fetch closure cannot reach that closure, so it cannot catch a
//! future hoist. Driving the real binary over a real paid channel can — and does:
//! a multi-chunk reconstruction only *succeeds* if each chunk advanced the nonce,
//! which is exactly the regression guard.
//!
//! Shape: onboard a provider node serving `[manifest, chunk0, chunk1, chunk2]`,
//! provision a funded buyer with a keystore, then run `decdn fetch <manifest>`
//! (retrying until the node's chain watcher has observed the freshly-opened
//! channel). Assert (a) the reconstructed file equals the chunk concatenation and
//! (b) the persisted channel watermark advanced past a single pull — i.e. every
//! chunk was paid for on the one shared channel.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH` and a prior build of the `decdn` binary:
//!
//! ```bash
//! cargo build -p decdn-cli
//! cargo nextest run -p decdn-e2e --features anvil-e2e cli_fetch_manifest
//! ```

#![cfg(feature = "anvil-e2e")]
// Test scaffolding legitimately uses unwrap/expect/panic; the workspace
// anti-panic policy targets runtime code.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::duration_suboptimal_units,
    // One sequential end-to-end journey reads more clearly unsplit.
    clippy::too_many_lines
)]

use std::path::PathBuf;
use std::time::Duration;

use alloy::primitives::U256;
use anyhow::Context;
use decdn_cache::Hash;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::node::NodeFixture;
use decdn_incentive::buyer_channel::BuyerChannelStore;
use decdn_incentive::buyer_channel_redb::RedbBuyerChannelStore;
use decdn_incentive::eth_identity::{self, KEYSTORE_PASSWORD_ENV};

const DEPOSIT_MICRO_USDC: u64 = 10_000_000; // 10 USDC (>= deploy minDeposit)
const KEYSTORE_PASSWORD: &str = "manifest-e2e-password";
const OVERALL_TIMEOUT: Duration = Duration::from_secs(780);

/// A postcard mirror of `cli::commands::file_manifest::{FileManifest, ChunkEntry}`
/// — those types are `pub(crate)`, and there is no manifest *producer* yet (the
/// PR is consumer-only). Postcard is structural, so serializing this with the
/// same field order and types as ADR 012 § Manifest format yields byte-identical
/// bytes to what the CLI consumer decodes. If the layouts ever diverge, the CLI's
/// `decode` fails and this test fails loudly — the mirror is self-checking.
#[derive(serde::Serialize)]
struct WireManifest {
    magic: [u8; 8],
    version: u8,
    total_bytes: u64,
    mime_type: String,
    filename: String,
    chunks: Vec<WireChunk>,
}

#[derive(serde::Serialize)]
struct WireChunk {
    hash: [u8; 32],
    size: u64,
}

#[tokio::test(flavor = "multi_thread")]
async fn cli_fetch_reconstructs_a_chunked_manifest() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("cli fetch manifest e2e exceeded the overall timeout")??;
    Ok(())
}

async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let decdn = decdn_cli_bin()?;
    let chain = ChainFixture::launch().await?;

    // The file: three distinct chunks. Small enough to keep the journey fast; the
    // point is *multiple* sequential paid pulls on one channel, not chunk size.
    let chunks: [&[u8]; 3] = [
        b"chunk-zero-aaaaaaaa",
        b"chunk-one-bbbbbbbbb",
        b"chunk-two-ccccccccc",
    ];
    let expected: Vec<u8> = chunks.concat();
    let manifest_blob = encode_manifest(&chunks);
    let manifest_hash = Hash::new(&manifest_blob);

    // Seed the node with the manifest blob AND every chunk blob, so a real
    // `cdn/client/v1` pull serves each whole from the local store.
    let mut serve: Vec<&[u8]> = vec![manifest_blob.as_slice()];
    serve.extend_from_slice(&chunks);
    let (node, hashes) = NodeFixture::launch_with_blobs(&chain, "US", &serve).await?;
    anyhow::ensure!(
        hashes.first() == Some(&manifest_hash),
        "seeded manifest hash mismatch: {:?} vs {manifest_hash}",
        hashes.first()
    );

    // Funded buyer with an on-disk keystore under a `0o700` client data dir (the
    // `RedbBuyerChannelStore` the CLI opens enforces the mode; `tempdir` is
    // `0o755`).
    let client_dir = tempfile::tempdir().context("client tempdir")?;
    // The keystore writer and the buyer store both require an `0o700` data dir; a
    // umask of 022 leaves the tempdir at 0o755, so tighten it before writing.
    #[cfg(unix)]
    std::fs::set_permissions(
        client_dir.path(),
        std::os::unix::fs::PermissionsExt::from_mode(0o700),
    )
    .context("chmod client dir 0o700")?;
    eth_identity::generate_and_persist(client_dir.path(), KEYSTORE_PASSWORD, false)
        .context("generate buyer keystore")?;
    let keystore = eth_identity::keystore_path(client_dir.path());
    let buyer = eth_identity::load_signer(&keystore, KEYSTORE_PASSWORD).context("load buyer")?;
    let buyer_addr = buyer.address();
    chain.fund_eth(buyer_addr, 100).await?;
    chain
        .mint_usdc(
            buyer_addr,
            U256::from(DEPOSIT_MICRO_USDC) * U256::from(4u64),
        )
        .await
        .context("mint buyer USDC")?;

    let out = client_dir.path().join("reconstructed.bin");
    let args = fetch_argv(
        &chain,
        &node,
        &manifest_hash,
        client_dir.path(),
        &keystore,
        &out,
    );

    // The node accepts vouchers only once its chain watcher has decoded the
    // `ChannelOpened` event (~500ms poll). `decdn fetch` has no internal retry, so
    // the first run opens+records the channel and races the watcher (refused with
    // `NotFound`); later runs reuse that recorded channel and succeed once
    // observation lands. The keystore password reaches the child via the env
    // source `fetch` checks before prompting — `Command::env` sets it safely (no
    // `set_var`, which the workspace forbids).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let output = tokio::process::Command::new(&decdn)
            .arg("fetch")
            .args(&args)
            .env(KEYSTORE_PASSWORD_ENV, KEYSTORE_PASSWORD)
            // Isolate HOME so the child never reads this machine's real
            // `~/.decdn/node.toml` (its `blockchain.eth_keystore` would otherwise
            // override the flags) and writes chunk parts under the tempdir.
            .env("HOME", client_dir.path())
            .output()
            .await
            .context("spawn decdn fetch")?;
        if output.status.success() {
            break;
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "decdn fetch (manifest reconstruction) never succeeded; last stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        tracing::debug!(
            "fetch not ready; retrying after watcher catch-up:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        tokio::time::sleep(Duration::from_millis(750)).await;
    }

    // (a) The reconstructed file is exactly the chunks concatenated in order.
    let got = std::fs::read(&out).context("read reconstructed output")?;
    anyhow::ensure!(
        got == expected,
        "reconstructed file mismatch: {} bytes vs expected {}",
        got.len(),
        expected.len()
    );

    // (a2) Chunk parts landed under the *resolved* `--data-dir`, not a hardcoded
    // `~/.decdn/downloads`. `HOME` is isolated to this same tempdir above, so the
    // two locations are distinguishable: only honoring `--data-dir` puts them
    // here. Retention is on (no `--no-keep-blobs`), so they survive the run.
    // This is the only coverage that the flag reaches `downloads_root` at all —
    // both call sites regress silently otherwise.
    let part = client_dir
        .path()
        .join("downloads")
        // `to_hex`, not `Display`: `iroh_blobs::Hash` displays as base32, while
        // the part directory is named with lowercase hex.
        .join(manifest_hash.to_hex())
        .join("chunk-0.part");
    anyhow::ensure!(
        part.is_file(),
        "chunk part not under the resolved --data-dir: {}",
        part.display()
    );

    // (b) The persisted watermark advanced past a single pull. The manifest blob
    // and all three chunks were paid on one channel; a hoisted context would have
    // failed the fetch outright above, so reaching here already proves per-chunk
    // signing — the nonce floor is the belt to that braces (>= one voucher per
    // chunk, ignoring the manifest pull).
    let store = RedbBuyerChannelStore::open(client_dir.path()).context("reopen buyer store")?;
    let state = store
        .get_by_provider(node.operator_addr())
        .context("read persisted channel")?
        .ok_or_else(|| anyhow::anyhow!("no buyer channel persisted after fetch"))?;
    anyhow::ensure!(
        state.last_nonce >= U256::from(chunks.len() as u64),
        "watermark did not advance per chunk: last_nonce = {}",
        state.last_nonce
    );

    // `node` (and `chain`) tear down on drop.
    Ok(())
}

/// Postcard-encode a `DECDNMAN` manifest over `chunks` (each hashed with BLAKE3),
/// matching the wire layout the CLI consumer decodes.
fn encode_manifest(chunks: &[&[u8]]) -> Vec<u8> {
    let entries: Vec<WireChunk> = chunks
        .iter()
        .map(|c| WireChunk {
            hash: *Hash::new(c).as_bytes(),
            size: c.len() as u64,
        })
        .collect();
    let manifest = WireManifest {
        magic: *b"DECDNMAN",
        version: 1,
        total_bytes: entries.iter().map(|e| e.size).sum(),
        mime_type: String::new(),
        filename: String::new(),
        chunks: entries,
    };
    postcard::to_allocvec(&manifest).expect("encode DECDNMAN manifest")
}

/// The `decdn fetch` argv (after the `fetch` subcommand) to pull `manifest_hash`
/// from `node` over the explicit-node path (`--node-id` + `--addr` +
/// `--provider-address`), with chain coordinates passed as flags so no config
/// file is needed. `--capacity-bond-address` is omitted deliberately — the node
/// holds every blob, so the (unbound) fetch never needs reactive origin
/// pull-through.
fn fetch_argv(
    chain: &ChainFixture,
    node: &NodeFixture,
    manifest_hash: &Hash,
    data_dir: &std::path::Path,
    keystore: &std::path::Path,
    out: &std::path::Path,
) -> Vec<String> {
    vec![
        "--hash".into(),
        manifest_hash.to_hex(),
        "-o".into(),
        out.display().to_string(),
        "--node-id".into(),
        node.node_id().to_string(),
        "--addr".into(),
        format!("127.0.0.1:{}", node.bind_port()),
        "--provider-address".into(),
        format!("{}", node.operator_addr()),
        "--rpc-url".into(),
        chain.rpc_url(),
        "--payment-channel-address".into(),
        format!("{}", chain.addrs().payment_channel),
        "--slash-judge-address".into(),
        format!("{}", chain.addrs().slash_judge),
        "--chain-id".into(),
        chain.chain_id().to_string(),
        "--data-dir".into(),
        data_dir.display().to_string(),
        "--keystore".into(),
        keystore.display().to_string(),
        "--deposit-micro-usdc".into(),
        DEPOSIT_MICRO_USDC.to_string(),
    ]
}

/// Locate the built `decdn` binary relative to the current test executable
/// (`target/<profile>/decdn`), falling back to `DECDN_CLI_BIN`. Mirrors the
/// node fixture's `decdn_node_bin`.
fn decdn_cli_bin() -> anyhow::Result<PathBuf> {
    if let Some(p) = std::env::var_os("DECDN_CLI_BIN") {
        return Ok(PathBuf::from(p));
    }
    let exe = std::env::current_exe().context("current_exe")?;
    // .../target/<profile>/deps/<test-bin>  → .../target/<profile>/decdn
    let profile_dir = exe
        .parent()
        .and_then(|deps| deps.parent())
        .context("resolve target profile dir")?;
    let bin = profile_dir.join(if cfg!(windows) { "decdn.exe" } else { "decdn" });
    anyhow::ensure!(
        bin.exists(),
        "decdn binary not found at {}; run `cargo build -p decdn-cli` first \
         (or set DECDN_CLI_BIN)",
        bin.display()
    );
    Ok(bin)
}
