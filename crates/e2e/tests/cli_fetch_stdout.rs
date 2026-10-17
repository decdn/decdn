//! Live anvil-backed e2e for `decdn get decdn:<hash> -o -` (#1848 4b) and
//! `decdn get decdn:<manifest>/<path> -o -`: the CLI's STDOUT streaming path
//! over the `decdn_client::Streamer` consumption face, driving the shipped
//! `decdn` binary against a real node over a real paid `cdn/client/v1` stream.
//! The entry form reads the bundle manifest into memory, then streams that one
//! entry.
//!
//! Why the whole binary rather than a unit test: the `Streamer` /
//! `VerifiedReader` unit tests in `decdn-client` already prove verified,
//! consumption-paced streaming against a `ScriptedSource`. What only exists on
//! the real wire is that the CLI routes `-o -` to the blob fetch, opens a
//! real `(signer, provider)` payment lane, streams the bao-verified bytes to the
//! process's stdout (nothing but verified bytes — safe to pipe), and persists
//! the lane's voucher watermark afterward exactly like the file path.
//!
//! What this test asserts:
//!   1. The captured stdout is byte-identical to the source blob. For the entry
//!      form, the manifest names a decoy entry before the target, so stdout
//!      shows the fetch picked the named entry.
//!   2. The client's persisted buyer-pool store shows a cumulative watermark
//!      against the holder of at least every byte fetched (the manifest
//!      included) — each stream was paid for and settled.
//!   3. The run reads the keystore password once, though the entry form
//!      fetches twice.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH` and a prior build of both `decdn` and `decdn-node`:
//!
//! ```bash
//! cargo build -p decdn-cli -p decdn-node
//! cargo nextest run -p decdn-e2e --features anvil-e2e cli_fetch_stdout
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
use decdn_cache::Hash;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::cli::{decdn_command, ensure_decdn_cli_built};
use decdn_e2e::node::NodeFixture;
use decdn_incentive::buyer_pool::BuyerPoolStore;
use decdn_incentive::buyer_pool_redb::RedbBuyerPoolStore;
use decdn_incentive::eth_identity;

const DEPOSIT_MICRO_USDC: u64 = 10_000_000; // 10 USDC (ADR 003 recommended minimum)
const KEYSTORE_PASSWORD: &str = "stdout-stream-e2e-password";
/// Standard journey tier (see [`decdn_e2e::timeout`]): one daemon plus a chain.
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::STANDARD;

/// Bytes per bao chunk group (matches `decdn_bao_range::IROH_BLOCK_SIZE`).
const CHUNK_GROUP: usize = 16 * 1024;

/// Deterministic pseudo-random blob spanning many chunk groups plus a ragged
/// final group, so the streamed output exercises multi-group bao verification.
fn make_blob() -> Vec<u8> {
    let mut v = vec![0u8; 20 * CHUNK_GROUP + 321];
    let mut x: u32 = 0x5EED_1848;
    for b in &mut v {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes().first().copied().unwrap_or(0);
    }
    v
}

#[tokio::test(flavor = "multi_thread")]
async fn cli_fetch_dash_streams_verified_bytes_to_stdout() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run(Target::Blob)))
        .await
        .context("cli get -o - e2e exceeded the overall timeout")??;
    Ok(())
}

/// `decdn get decdn:<manifest>/<path> -o -`: the CLI reads the manifest, then
/// streams that one entry's verified bytes to stdout.
#[tokio::test(flavor = "multi_thread")]
async fn cli_get_entry_dash_streams_the_entry_to_stdout() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run(Target::Entry)))
        .await
        .context("cli get entry -o - e2e exceeded the overall timeout")??;
    Ok(())
}

/// What the URI names: the blob itself, or the blob as an entry of a bundle.
#[derive(Clone, Copy)]
enum Target {
    Blob,
    Entry,
}

/// The manifest path the entry test names.
const ENTRY_PATH: &str = "clips/clip.bin";

/// A manifest path listed before [`ENTRY_PATH`], whose blob no node holds.
const DECOY_PATH: &str = "clips/decoy.bin";

/// The warning each password read prints for the unused password file the
/// argv names, since the env password takes precedence.
const UNUSED_PASSWORD_FILE: &str = "was not used";

async fn run(target: Target) -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    ensure_decdn_cli_built()?;
    let chain = ChainFixture::launch().await?;

    let blob = make_blob();
    let blob_hash = Hash::new(&blob);
    let manifest = format!(
        r#"{{"version":1,"entries":[{{"path":"{DECOY_PATH}","hash":"{}"}},{{"path":"{ENTRY_PATH}","hash":"{}"}}]}}"#,
        Hash::new(b"decoy").to_hex(),
        blob_hash.to_hex(),
    )
    .into_bytes();
    let manifest_hash = Hash::new(&manifest);
    let (holder, hashes) =
        NodeFixture::launch_with_blobs(&chain, "US", &[blob.as_slice(), manifest.as_slice()])
            .await?;
    anyhow::ensure!(
        hashes == [blob_hash, manifest_hash],
        "holder seeded hashes mismatch: {hashes:?} vs [{blob_hash}, {manifest_hash}]"
    );
    let uri = match target {
        Target::Blob => format!("decdn:{}", blob_hash.to_hex()),
        Target::Entry => format!("decdn:{}/{ENTRY_PATH}", manifest_hash.to_hex()),
    };

    // Funded buyer with an on-disk keystore under a `0o700` client data dir (the
    // `RedbBuyerPoolStore` the CLI opens enforces the mode; `tempdir` is `0o755`).
    let client_dir = tempfile::tempdir().context("client tempdir")?;
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
            U256::from(DEPOSIT_MICRO_USDC) * U256::from(8u64),
        )
        .await
        .context("mint buyer USDC")?;

    let args = fetch_argv(&chain, &uri, client_dir.path(), &keystore);

    // The node's chain watcher observes the freshly-opened pool asynchronously; a
    // pre-serve refusal before it catches up is retryable, so retry until one
    // invocation streams the whole blob to stdout.
    let (stdout, stderr) = run_fetch_until_streamed(client_dir.path(), &args, blob.len()).await?;

    // (1) stdout is byte-identical to the source blob — nothing but verified
    // bytes reached the pipe.
    anyhow::ensure!(
        stdout == blob,
        "streamed {} bytes to stdout, expected {}",
        stdout.len(),
        blob.len()
    );

    // (2) The client's persisted store shows a watermark against the holder of
    // at least every byte fetched — each stream was paid for and settled,
    // exactly like the file path.
    let fetched = match target {
        Target::Blob => blob.len(),
        Target::Entry => blob.len() + manifest.len(),
    };
    let billed = billed_bytes(client_dir.path(), holder.operator_addr())?;
    anyhow::ensure!(
        billed >= u64::try_from(fetched)?,
        "the client's persisted buyer-pool store shows {billed} bytes billed to the holder, \
         expected at least {fetched}"
    );

    // (3) One password read for the run, the entry form's two fetches included.
    let reads = stderr.matches(UNUSED_PASSWORD_FILE).count();
    anyhow::ensure!(
        reads == 1,
        "the run read the keystore password {reads} times, expected once; stderr:\n{stderr}"
    );

    drop(holder);
    Ok(())
}

/// Run `decdn get ... -o -` until one invocation streams the whole blob to
/// stdout, returning that invocation's stdout bytes and stderr text.
async fn run_fetch_until_streamed(
    data_dir: &std::path::Path,
    args: &[String],
    expected_len: usize,
) -> anyhow::Result<(Vec<u8>, String)> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let output = tokio::process::Command::from(decdn_command(data_dir, KEYSTORE_PASSWORD)?)
            .arg("get")
            .args(args)
            .output()
            .await
            .context("spawn decdn get -o -")?;
        if output.status.success() && output.stdout.len() == expected_len {
            return Ok((
                output.stdout,
                String::from_utf8_lossy(&output.stderr).into_owned(),
            ));
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "no `decdn get -o -` streamed the whole blob; last stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        tracing::debug!(
            "get -o - not ready (status {:?}, {} stdout bytes); retrying:\n{}",
            output.status.code(),
            output.stdout.len(),
            String::from_utf8_lossy(&output.stderr)
        );
        tokio::time::sleep(Duration::from_millis(750)).await;
    }
}

/// Cumulative bytes the buyer has paid `provider` for, from the persisted pool's
/// `(signer, provider)` lane watermark. Reopened per call because the CLI
/// subprocess owns the store between calls.
fn billed_bytes(
    data_dir: &std::path::Path,
    provider: alloy::primitives::Address,
) -> anyhow::Result<u64> {
    let Ok(store) = RedbBuyerPoolStore::open(data_dir) else {
        return Ok(0);
    };
    let mut billed = U256::ZERO;
    for pool in store.load_all().context("load buyer pools")?.pools {
        for (lane, progress) in pool.lanes() {
            if lane.provider == provider {
                billed = billed.max(progress.last_bytes);
            }
        }
    }
    Ok(u64::try_from(billed).unwrap_or(u64::MAX))
}

/// The `decdn get` argv (after the `get` subcommand) to stream what `uri`
/// names to stdout via auto-discovery, writing to `-o -`. It names a password
/// file that does not exist, so each password read warns that the env password
/// took precedence.
fn fetch_argv(
    chain: &ChainFixture,
    uri: &str,
    data_dir: &std::path::Path,
    keystore: &std::path::Path,
) -> Vec<String> {
    vec![
        uri.to_owned(),
        "-o".into(),
        "-".into(),
        "--capacity-bond-address".into(),
        format!("{}", chain.addrs().capacity_bond),
        "--rpc-url".into(),
        chain.rpc_url(),
        "--payment-pool-address".into(),
        format!("{}", chain.addrs().payment_pool),
        "--slash-judge-address".into(),
        format!("{}", chain.addrs().slash_judge),
        "--chain-id".into(),
        chain.chain_id().to_string(),
        "--data-dir".into(),
        data_dir.display().to_string(),
        "--keystore".into(),
        keystore.display().to_string(),
        "--keystore-password-file".into(),
        data_dir.join("unused-password").display().to_string(),
        "--working-deposit-micro-usdc".into(),
        DEPOSIT_MICRO_USDC.to_string(),
    ]
}
