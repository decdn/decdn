//! Live anvil-backed e2e for `decdn quote` (#2600): the shipped `decdn` binary
//! prices a blob and a bundle from the rates two real nodes sign in their
//! `cdn/probe/v1` answers.
//!
//! The unit tests in `decdn-client` and the CLI prove the ranking and the
//! price math against hand-built answers. What only the real wire proves is
//! that the census reaches every registered node, verifies each signed rate
//! against the operator the registry names, and reads the rate a daemon was
//! configured with; and that a bundle quote's paid manifest fetch caches the
//! manifest for the `get` that follows.
//!
//! Two nodes hold the same blob, one at 8 and one at 12 micro-USDC per MiB.
//! The cheap node also holds a small blob and a bundle manifest naming both.
//! What this test asserts:
//!   1. `decdn quote --json` sees both holders, both rates, and prices the
//!      blob's wire bytes at each.
//!   2. `--max-rate-per-mb 10` leaves the more expensive node out of the
//!      price.
//!   3. A pinned `--node-id` quote probes that node alone.
//!   4. `decdn get --dry-run` prints the quote block, and under `--json`
//!      carries the quote, and writes no output file.
//!   5. A bundle quote with no cached manifest and no open pool stops
//!      before any transaction and names `--open-pool` (#2675). With
//!      `--open-pool`, it says it fetches the manifest, opens a pool, pays
//!      for the manifest, caches it, and prices both entries; a second quote
//!      and `get --dry-run --json` read the cache. A quote into a fresh
//!      directory then fetches the manifest from that pool with no flag, and
//!      sends no transaction.
//!   6. `decdn get` of the blob prints exactly one `Estimate:` line before
//!      its pull.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH` and a prior build of both `decdn` and `decdn-node`:
//!
//! ```bash
//! cargo build -p decdn-cli -p decdn-node
//! cargo nextest run -p decdn-e2e --features anvil-e2e cli_quote
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

use std::path::Path;
use std::time::Duration;

use alloy::primitives::U256;
use anyhow::Context;
use decdn_cache::Hash;
use decdn_client::bundle::BundleManifest;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::cli::{decdn_command, ensure_decdn_cli_built};
use decdn_e2e::node::NodeFixture;
use decdn_incentive::eth_identity;
use decdn_incentive::rate::min_payment;

const DEPOSIT_MICRO_USDC: u64 = 10_000_000; // 10 USDC (ADR 003 recommended minimum)
const KEYSTORE_PASSWORD: &str = "quote-e2e-password";
/// Standard journey tier (see [`decdn_e2e::timeout`] for the tier rule): two
/// daemons plus a chain, and two small paid pulls.
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::STANDARD;
const CHEAP_RATE: u64 = 8;
const DEAR_RATE: u64 = 12;

/// A deterministic blob of `len` bytes from `seed`.
fn make_blob(len: u32, seed: u32) -> Vec<u8> {
    (0..len)
        .map(|i| (i ^ seed).wrapping_mul(2_654_435_761).to_le_bytes()[3])
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn cli_quote_prices_from_signed_probe_rates() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("cli quote e2e exceeded the overall timeout")??;
    Ok(())
}

/// What one `decdn` run printed.
struct Ran {
    stdout: String,
    stderr: String,
}

/// The journey's chain, nodes, buyer and content.
struct Journey {
    chain: ChainFixture,
    client_dir: tempfile::TempDir,
    keystore: std::path::PathBuf,
    /// The buyer key, to read its transaction count.
    buyer: alloy::signers::local::PrivateKeySigner,
    /// The cheap node: holds the blob, the small blob and the manifest.
    cheap: NodeFixture,
    /// The more expensive node: holds the blob. Kept so it keeps serving.
    _dear: NodeFixture,
    blob: Vec<u8>,
    blob_hash: Hash,
    small_len: usize,
    manifest_hash: Hash,
}

impl Journey {
    /// Run `decdn <argv>` as the journey's buyer ([`run_decdn`]).
    fn decdn(&self, argv: &[&str]) -> anyhow::Result<Ran> {
        let (ok, ran) = run_decdn(self.client_dir.path(), &self.keystore, &self.chain, argv)?;
        anyhow::ensure!(ok, "decdn {argv:?} failed: {}", ran.stderr);
        Ok(ran)
    }

    /// Run `decdn <argv>` as the journey's buyer, and expect it to fail.
    fn decdn_failing(&self, argv: &[&str]) -> anyhow::Result<Ran> {
        let (ok, ran) = run_decdn(self.client_dir.path(), &self.keystore, &self.chain, argv)?;
        anyhow::ensure!(!ok, "decdn {argv:?} succeeded: {}", ran.stderr);
        Ok(ran)
    }

    /// How many transactions the buyer has sent: its nonce.
    async fn buyer_nonce(&self) -> anyhow::Result<u64> {
        use alloy::providers::Provider as _;
        self.chain
            .provider_for(&self.buyer)
            .get_transaction_count(self.buyer.address())
            .await
            .context("read the buyer's nonce")
    }

    /// The blob's URI.
    fn uri(&self) -> String {
        format!("decdn:{}", self.blob_hash.to_hex())
    }
}

async fn run() -> anyhow::Result<()> {
    let j = launch().await?;
    blob_quotes(&j)?;
    dry_runs(&j)?;
    bundle_quote(&j).await?;
    get_estimate(&j)
}

/// Two nodes at two rates, and a funded buyer.
async fn launch() -> anyhow::Result<Journey> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();
    ensure_decdn_cli_built()?;
    let chain = ChainFixture::launch().await?;

    // ~2 MiB, so the price runs past one MiB.
    let blob = make_blob(2 * 1024 * 1024 + 4321, 1);
    let small = make_blob(70_000, 2);
    let blob_hash = Hash::new(&blob);
    let small_hash = Hash::new(&small);
    let manifest = format!(
        r#"{{"version":1,"entries":[{{"path":"big.bin","hash":"{}","size":{}}},{{"path":"small.bin","hash":"{}","size":{}}}]}}"#,
        blob_hash.to_hex(),
        blob.len(),
        small_hash.to_hex(),
        small.len(),
    )
    .into_bytes();
    let manifest_hash = Hash::new(&manifest);
    let (cheap, _) = NodeFixture::launch_with_blobs(
        &chain,
        "US",
        &[blob.as_slice(), small.as_slice(), manifest.as_slice()],
    )
    .await?;
    let (dear, _) = NodeFixture::launch_with_blobs(&chain, "US", &[blob.as_slice()]).await?;
    cheap.set_rate_per_mb(CHEAP_RATE).await?;
    dear.set_rate_per_mb(DEAR_RATE).await?;

    // A funded buyer, for the bundle's manifest fetch and the paid `get`.
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
    chain.fund_eth(buyer.address(), 100).await?;
    chain
        .mint_usdc(
            buyer.address(),
            U256::from(DEPOSIT_MICRO_USDC) * U256::from(4u64),
        )
        .await
        .context("mint buyer USDC")?;
    Ok(Journey {
        chain,
        client_dir,
        keystore,
        buyer,
        cheap,
        _dear: dear,
        blob_hash,
        small_len: small.len(),
        manifest_hash,
        blob,
    })
}

/// Steps 1–3: the blob's quote, capped, and pinned.
fn blob_quotes(j: &Journey) -> anyhow::Result<()> {
    let uri = j.uri();
    let wire = decdn_client::quote::wire_bytes(u64::try_from(j.blob.len())?)?;
    // (1) Both holders, both signed rates, priced at each.
    let quote = json(&j.decdn(&["quote", &uri, "--json"])?)?;
    assert_eq!(quote["target"], "blob");
    assert_eq!(quote["content_bytes"], j.blob.len());
    assert_eq!(quote["nodes_probed"], 2, "{quote}");
    assert_eq!(quote["holders_min"], 2, "{quote}");
    assert_eq!(quote["rate_min_per_mib"], CHEAP_RATE, "{quote}");
    assert_eq!(quote["rate_max_per_mib"], DEAR_RATE, "{quote}");
    let price = &quote["price_micro_usdc"];
    assert_eq!(price["low"], min_payment(wire, CHEAP_RATE).to_string());
    assert_eq!(price["high"], min_payment(wire, DEAR_RATE).to_string());
    // Two lanes, one at each rate, split evenly.
    let likely = min_payment(wire - wire / 2, CHEAP_RATE) + min_payment(wire / 2, DEAR_RATE);
    let likely_alt = min_payment(wire - wire / 2, DEAR_RATE) + min_payment(wire / 2, CHEAP_RATE);
    let got = price["likely"].as_str().unwrap_or_default().to_owned();
    assert!(
        got == likely.to_string() || got == likely_alt.to_string(),
        "{quote}"
    );

    // (2) A ceiling between the two rates leaves the more expensive node out.
    let capped = json(&j.decdn(&["quote", &uri, "--json", "--max-rate-per-mb", "10"])?)?;
    assert_eq!(capped["rate_max_per_mib"], CHEAP_RATE, "{capped}");
    assert_eq!(capped["above_ceiling"], 1, "{capped}");
    assert_eq!(
        capped["price_micro_usdc"]["high"],
        min_payment(wire, CHEAP_RATE).to_string(),
        "{capped}"
    );

    // (3) A pinned node is probed alone.
    let node_id = j.cheap.node_id().to_string();
    let provider = j.cheap.operator_addr().to_string();
    let addr = format!("127.0.0.1:{}", j.cheap.bind_port());
    let pinned = json(&j.decdn(&[
        "quote",
        &uri,
        "--json",
        "--node-id",
        &node_id,
        "--provider-address",
        &provider,
        "--addr",
        &addr,
    ])?)?;
    assert_eq!(pinned["nodes_probed"], 1, "{pinned}");
    assert_eq!(pinned["rate_max_per_mib"], CHEAP_RATE, "{pinned}");

    Ok(())
}

/// Step 4: `get --dry-run` prints the quote, and writes nothing.
fn dry_runs(j: &Journey) -> anyhow::Result<()> {
    let uri = j.uri();
    // (4) `get --dry-run` prints the quote block, and writes nothing.
    let out = j.client_dir.path().join("blob.bin");
    let out_arg = out.display().to_string();
    let dry = j.decdn(&["get", &uri, "--dry-run", "-o", &out_arg])?;
    assert!(dry.stdout.contains("would fetch blob"), "{}", dry.stdout);
    assert!(dry.stdout.contains("2 of 2 hold it"), "{}", dry.stdout);
    assert!(
        dry.stdout.contains("rates 0.0076–0.0114 USDC/GB"),
        "{}",
        dry.stdout
    );
    let dry_json = json(&j.decdn(&["get", &uri, "--dry-run", "--json", "-o", &out_arg])?)?;
    assert_eq!(dry_json["quote"]["target"], "blob", "{dry_json}");
    assert_eq!(dry_json["quote_error"], serde_json::Value::Null);
    assert_eq!(dry_json["hash"], j.blob_hash.to_hex());
    anyhow::ensure!(!out.exists(), "a dry run wrote {}", out.display());

    Ok(())
}

/// Step 5: a bundle quote opens no pool without `--open-pool`, pays for the
/// manifest once and caches it, and reuses the pool it opened.
async fn bundle_quote(j: &Journey) -> anyhow::Result<()> {
    let bundle_dir = j.client_dir.path().join("bundle");
    let bundle_arg = bundle_dir.display().to_string();
    let bundle_uri = format!("decdn:{}/", j.manifest_hash.to_hex());
    let cached = BundleManifest::cache_path(&bundle_dir, *j.manifest_hash.as_bytes());

    // (5a) No open pool and no `--open-pool`: the quote stops before any
    // transaction, and says what opening one costs.
    anyhow::ensure!(
        j.buyer_nonce().await? == 0,
        "the buyer has sent nothing yet"
    );
    let refused = j.decdn_failing(&["quote", &bundle_uri, "-o", &bundle_arg, "--json"])?;
    for part in [
        "no open pool pays for the manifest fetch",
        "escrows the working deposit (10.00 USDC)",
        "pays gas",
        "--open-pool`",
        "decdn pool open",
    ] {
        assert!(
            refused.stderr.contains(part),
            "{part:?}: {}",
            refused.stderr
        );
    }
    assert!(refused.stdout.trim().is_empty(), "{}", refused.stdout);
    assert_eq!(
        j.buyer_nonce().await?,
        0,
        "the refused quote sent a transaction"
    );
    assert!(!cached.exists(), "the refused quote cached a manifest");

    // (5b) `--open-pool`: the quote opens a pool, pays for the manifest once
    // and caches it.
    let first = j.decdn(&[
        "quote",
        &bundle_uri,
        "-o",
        &bundle_arg,
        "--json",
        "--open-pool",
    ])?;
    assert!(
        first.stderr.contains("no cached manifest"),
        "{}",
        first.stderr
    );
    let bundle = json(&first)?;
    assert_eq!(bundle["target"], "bundle", "{bundle}");
    assert_eq!(bundle["entries"], 2, "{bundle}");
    assert_eq!(
        bundle["content_bytes"],
        j.blob.len() + j.small_len,
        "{bundle}"
    );
    assert_eq!(bundle["sampled_blobs"], 2, "{bundle}");
    assert!(bundle["price_micro_usdc"].is_object(), "{bundle}");
    anyhow::ensure!(
        cached.exists(),
        "no cached manifest at {}",
        cached.display()
    );
    let opened = j.buyer_nonce().await?;
    anyhow::ensure!(opened > 0, "the quote opened no pool");
    let second = j.decdn(&["quote", &bundle_uri, "-o", &bundle_arg, "--json"])?;
    assert!(
        !second.stderr.contains("no cached manifest"),
        "{}",
        second.stderr
    );
    let plan = json(&j.decdn(&["get", &bundle_uri, "-o", &bundle_arg, "--dry-run", "--json"])?)?;
    assert_eq!(plan["count"], 2, "{plan}");
    assert_eq!(plan["quote"]["target"], "bundle", "{plan}");

    // (5c) An open pool and no flag: a quote into a fresh directory fetches
    // the manifest from that pool, and sends no transaction.
    let fresh_dir = j.client_dir.path().join("bundle-again");
    let fresh_arg = fresh_dir.display().to_string();
    let from_pool = j.decdn(&["quote", &bundle_uri, "-o", &fresh_arg, "--json"])?;
    assert!(
        from_pool.stderr.contains("no cached manifest"),
        "{}",
        from_pool.stderr
    );
    assert_eq!(json(&from_pool)?["entries"], 2, "{}", from_pool.stdout);
    assert!(
        BundleManifest::cache_path(&fresh_dir, *j.manifest_hash.as_bytes()).exists(),
        "the quote cached no manifest in {fresh_arg}"
    );
    assert_eq!(
        j.buyer_nonce().await?,
        opened,
        "the quote sent a transaction with an open pool"
    );

    Ok(())
}

/// Step 6: a paid `get` prints its estimate once, before the pull.
fn get_estimate(j: &Journey) -> anyhow::Result<()> {
    let uri = j.uri();
    let out = j.client_dir.path().join("blob.bin");
    let out_arg = out.display().to_string();
    // (6) A paid `get` prints its estimate once, before the pull.
    let got = j.decdn(&["get", &uri, "-o", &out_arg])?;
    let estimates = got
        .stderr
        .lines()
        .filter(|l| l.starts_with("Estimate: "))
        .count();
    assert_eq!(estimates, 1, "{}", got.stderr);
    assert_eq!(std::fs::read(&out)?, j.blob);
    Ok(())
}

/// The JSON a run printed on stdout.
fn json(ran: &Ran) -> anyhow::Result<serde_json::Value> {
    serde_json::from_str(ran.stdout.trim()).with_context(|| format!("parse JSON: {}", ran.stdout))
}

/// Run `decdn <argv> <chain flags>` against `chain` with `data_dir` as its
/// home and data dir and `keystore` as its buyer key: whether it exited zero,
/// and what it printed.
fn run_decdn(
    data_dir: &Path,
    keystore: &Path,
    chain: &ChainFixture,
    argv: &[&str],
) -> anyhow::Result<(bool, Ran)> {
    let output = decdn_command(data_dir, KEYSTORE_PASSWORD)?
        .args(argv)
        .args([
            "--capacity-bond-address".to_owned(),
            format!("{}", chain.addrs().capacity_bond),
            "--rpc-url".to_owned(),
            chain.rpc_url(),
            "--payment-pool-address".to_owned(),
            format!("{}", chain.addrs().payment_pool),
            "--slash-judge-address".to_owned(),
            format!("{}", chain.addrs().slash_judge),
            "--chain-id".to_owned(),
            chain.chain_id().to_string(),
            "--data-dir".to_owned(),
            data_dir.display().to_string(),
            "--keystore".to_owned(),
            keystore.display().to_string(),
            "--working-deposit-micro-usdc".to_owned(),
            DEPOSIT_MICRO_USDC.to_string(),
        ])
        .output()
        .context("spawn decdn")?;
    Ok((
        output.status.success(),
        Ran {
            stdout: String::from_utf8(output.stdout)?,
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        },
    ))
}
