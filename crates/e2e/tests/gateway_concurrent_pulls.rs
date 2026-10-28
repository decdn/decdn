//! Live anvil-backed e2e for `decdn gateway`'s concurrent pulls (#2458).
//!
//! Shape: an owner opens a pool, a node serves two blobs, and a gateway pinned
//! to that node runs as a subprocess. The gateway mints a delegate key, the
//! owner issues a capability to it (`decdn pool assign`), and the gateway
//! registers it. Then:
//!
//!   1. Four requests under that one capability run at once, each blob twice.
//!      Every pull pays on the same `(pool, delegate, operator)` lane. The
//!      gateway advances the lane's cumulative through one issuer, so the node
//!      rejects none of their vouchers; with a ledger per pull, a pull's
//!      cumulative trails its sibling's and the node rejects it
//!      (`AmountRegression`) until the pull reseeds. All four bodies must
//!      arrive whole, with no voucher rejected.
//!   2. A request whose body is never read holds only its own pull: another
//!      request still streams to its end meanwhile.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH` and built `decdn` and `decdn-node` binaries:
//!
//! ```bash
//! cargo build -p decdn-cli -p decdn-node
//! cargo nextest run -p decdn-e2e --features anvil-e2e gateway_concurrent_pulls
//! ```

#![cfg(feature = "anvil-e2e")]
// Test scaffolding legitimately uses unwrap/expect/panic; the workspace
// anti-panic policy targets runtime code.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::similar_names
)]

use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

use alloy::primitives::{B256, U256};
use anyhow::Context;
use decdn_cache::Hash;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::cli::{decdn_command, ensure_decdn_cli_built};
use decdn_e2e::node::NodeFixture;
use decdn_e2e::ports::PortReservation;
use decdn_incentive::eth_identity;

const MIB: usize = 1024 * 1024;
/// The owner's pool deposit: 10 USDC.
const DEPOSIT_MICRO_USDC: u64 = 10_000_000;
/// The delegate's cap: 5 USDC, far above what the pulls cost.
const CAP_MICRO_USDC: u64 = 5_000_000;
const KEYSTORE_PASSWORD: &str = "gateway-concurrent-e2e-password";
/// The page host the content requests serve. The capability's `*` scope
/// covers it.
const PAGE_HOST: &str = "example.com";
/// The node's count of paying streams it stopped over a rejected voucher.
const VOUCHER_REJECTED: &str = "decdn_serve_stream_voucher_rejected_total";
/// Standard journey tier (see [`decdn_e2e::timeout`] for the tier rule).
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::STANDARD;

/// Deterministic pseudo-random bytes (xorshift32), distinct per `seed`.
fn blob(len: usize, seed: u32) -> Vec<u8> {
    let mut v = vec![0u8; len];
    let mut x = seed;
    for b in &mut v {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes().first().copied().unwrap_or(0);
    }
    v
}

#[tokio::test(flavor = "multi_thread")]
async fn gateway_runs_pulls_under_one_capability_at_once() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("gateway concurrent-pulls e2e exceeded the overall timeout")??;
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "one sequential journey: pool, gateway, capability, then the pulls that depend on \
              all three"
)]
async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();
    ensure_decdn_cli_built()?;
    let chain = ChainFixture::launch().await?;

    // Two 4 MiB blobs: each pull spans several one-MiB voucher intervals, so
    // two pulls on one lane interleave their vouchers. A third, small blob warms
    // the node up.
    let a = blob(4 * MIB, 0xA11C_E001);
    let b = blob(4 * MIB, 0xB0B0_0002);
    let warm = blob(64 * 1024, 0x0C0F_FEE3);
    let (node, hashes) = NodeFixture::launch_with_blobs(
        &chain,
        "US",
        &[a.as_slice(), b.as_slice(), warm.as_slice()],
    )
    .await?;
    let (hash_a, hash_b, hash_warm) = (hashes[0], hashes[1], hashes[2]);

    // ---- OWNER: a funded keystore that opens the pool ----
    let owner_dir = private_tempdir("owner")?;
    eth_identity::generate_and_persist(owner_dir.path(), KEYSTORE_PASSWORD, false)
        .context("generate owner keystore")?;
    let owner_keystore = eth_identity::keystore_path(owner_dir.path());
    let owner = eth_identity::load_signer(&owner_keystore, KEYSTORE_PASSWORD)?.address();
    chain.fund_eth(owner, 100).await?;
    chain
        .mint_usdc(owner, U256::from(DEPOSIT_MICRO_USDC) * U256::from(4u64))
        .await?;
    let opened = decdn_ok(
        owner_dir.path(),
        &[
            strs(&[
                "pool",
                "open",
                "--deposit-micro-usdc",
                &DEPOSIT_MICRO_USDC.to_string(),
            ]),
            chain_flags(&chain),
            owner_flags(&owner_keystore, owner_dir.path()),
        ]
        .concat(),
    )
    .await?;
    let pool_id = parse_pool_id(&opened)?;

    // The owner's own fetch retries until the node's chain watcher has seen the
    // fresh pool. The gateway then never meets that race: a refusal it took for
    // a funding stop would retire the capability.
    warm_up(&chain, &node, owner_dir.path(), &owner_keystore, &hash_warm).await?;

    // ---- GATEWAY: pinned to the node, on a free loopback port ----
    let gateway_dir = private_tempdir("gateway")?;
    // Held until the test ends, so no other process takes the port first.
    let gateway_port = PortReservation::tcp()?;
    let listen = gateway_port.addr().to_string();
    let mut gateway = tokio::process::Command::from(decdn_command(gateway_dir.path(), "")?)
        .args(
            [
                strs(&[
                    "gateway",
                    "--listen",
                    &listen,
                    "--data-dir",
                    &gateway_dir.path().display().to_string(),
                ]),
                chain_flags(&chain),
                node_flags(&chain, &node),
            ]
            .concat(),
        )
        .kill_on_drop(true)
        .spawn()
        .context("spawn decdn gateway")?;
    let base = format!("http://{listen}");
    let token = wait_for_token(&gateway_dir.path().join("token"), &base).await?;
    let http = reqwest::Client::new();

    // ---- The gateway mints a delegate; the owner issues it a capability ----
    let minted: serde_json::Value = http
        .get(format!("{base}/_delegate"))
        .header("x-decdn-token", &token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let delegate = minted["address"]
        .as_str()
        .context("the minted delegate's address")?
        .to_owned();
    let assigned = decdn_ok(
        owner_dir.path(),
        &[
            strs(&[
                "pool",
                "assign",
                "--pool",
                &format!("{pool_id:#x}"),
                "--signer",
                &delegate,
                "--cap-micro-usdc",
                &CAP_MICRO_USDC.to_string(),
                "--expiry-secs",
                "3600",
            ]),
            chain_flags(&chain),
            owner_flags(&owner_keystore, owner_dir.path()),
        ]
        .concat(),
    )
    .await?;
    let capability = assigned
        .lines()
        .map(str::trim)
        .find(|line| line.starts_with("dcap1:"))
        .context("the dcap1: token in `pool assign` output")?;
    let registered = http
        .put(format!("{base}/_capability"))
        .header("x-decdn-token", &token)
        .json(&serde_json::json!({ "capability": capability, "scope": "*" }))
        .send()
        .await?;
    anyhow::ensure!(
        registered.status().is_success(),
        "register the capability: {} {}",
        registered.status(),
        registered.text().await.unwrap_or_default()
    );

    // ---- (1) Four pulls under one capability at once ----
    let rejected_before = node.scrape_metric(VOUCHER_REJECTED).await?;
    let get = |hash: Hash| {
        let request = http
            .get(format!("{base}/{}", hash.to_hex()))
            .header("x-decdn-token", &token)
            .header("x-decdn-host", PAGE_HOST);
        async move {
            let response = request.send().await?;
            let status = response.status();
            let body = response.bytes().await?;
            anyhow::ensure!(
                status.is_success(),
                "{status}: {}",
                String::from_utf8_lossy(&body)
            );
            anyhow::Ok(body.to_vec())
        }
    };
    let (got_a1, got_b1, got_a2, got_b2) =
        tokio::try_join!(get(hash_a), get(hash_b), get(hash_a), get(hash_b))?;
    for (name, got, want) in [
        ("a #1", &got_a1, &a),
        ("b #1", &got_b1, &b),
        ("a #2", &got_a2, &a),
        ("b #2", &got_b2, &b),
    ] {
        anyhow::ensure!(
            got == want,
            "{name}: got {} bytes, expected {} byte-exact",
            got.len(),
            want.len()
        );
    }
    let rejected = node
        .scrape_metric(VOUCHER_REJECTED)
        .await?
        .saturating_sub(rejected_before);
    anyhow::ensure!(
        rejected == 0,
        "the node rejected {rejected} voucher stream(s) on the shared lane: the pulls did not \
         advance its cumulative through one issuer"
    );

    // ---- (2) A response whose reader stops does not block another ----
    let stalled = http
        .get(format!("{base}/{}", hash_a.to_hex()))
        .header("x-decdn-token", &token)
        .header("x-decdn-host", PAGE_HOST)
        .send()
        .await?;
    anyhow::ensure!(stalled.status().is_success(), "{}", stalled.status());
    // Its body stays unread while the next request runs to its end.
    let got = tokio::time::timeout(Duration::from_secs(60), get(hash_b))
        .await
        .context("a request waited behind a stalled response")??;
    anyhow::ensure!(got == b, "b behind a stalled a: {} bytes", got.len());
    drop(stalled);

    gateway.start_kill().context("stop the gateway")?;
    let _ = gateway.wait().await;
    drop(node);
    Ok(())
}

fn strs(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| (*s).to_owned()).collect()
}

/// A fresh owner-only temp dir: the buyer store refuses one others can read.
fn private_tempdir(role: &str) -> anyhow::Result<tempfile::TempDir> {
    let dir = tempfile::tempdir().with_context(|| format!("{role} tempdir"))?;
    #[cfg(unix)]
    std::fs::set_permissions(
        dir.path(),
        std::os::unix::fs::PermissionsExt::from_mode(0o700),
    )
    .with_context(|| format!("chmod {role} dir 0o700"))?;
    Ok(dir)
}

/// The chain coordinates as flags, so no config file is needed.
fn chain_flags(chain: &ChainFixture) -> Vec<String> {
    vec![
        "--rpc-url".into(),
        chain.rpc_url(),
        "--payment-pool-address".into(),
        format!("{}", chain.addrs().payment_pool),
        "--chain-id".into(),
        chain.chain_id().to_string(),
    ]
}

/// The paid-pull flags that pin `node`. `--capacity-bond-address` is the
/// EIP-712 `verifyingContract` of the client identity binding a node requires.
fn node_flags(chain: &ChainFixture, node: &NodeFixture) -> Vec<String> {
    vec![
        "--node-id".into(),
        node.node_id().to_string(),
        "--addr".into(),
        format!("127.0.0.1:{}", node.bind_port()),
        "--provider-address".into(),
        format!("{}", node.operator_addr()),
        "--capacity-bond-address".into(),
        format!("{}", chain.addrs().capacity_bond),
        "--slash-judge-address".into(),
        format!("{}", chain.addrs().slash_judge),
    ]
}

fn owner_flags(keystore: &Path, data_dir: &Path) -> Vec<String> {
    vec![
        "--keystore".into(),
        keystore.display().to_string(),
        "--data-dir".into(),
        data_dir.display().to_string(),
    ]
}

/// Run `decdn <args>` as the owner, requiring success, and return its stdout.
async fn decdn_ok(home: &Path, args: &[String]) -> anyhow::Result<String> {
    let output = tokio::process::Command::from(decdn_command(home, KEYSTORE_PASSWORD)?)
        .args(args)
        .output()
        .await
        .context("spawn decdn")?;
    anyhow::ensure!(
        output.status.success(),
        "decdn {} failed: {}",
        args.first().map_or("", String::as_str),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(String::from_utf8(output.stdout)?)
}

/// The owner's own `decdn get` of `hash`, retried until the node serves its
/// fresh pool.
async fn warm_up(
    chain: &ChainFixture,
    node: &NodeFixture,
    home: &Path,
    keystore: &Path,
    hash: &Hash,
) -> anyhow::Result<()> {
    let out = home.join("warm.bin");
    let args = [
        vec![
            "get".to_owned(),
            format!("decdn:{}", hash.to_hex()),
            "-o".to_owned(),
            out.display().to_string(),
        ],
        chain_flags(chain),
        node_flags(chain, node),
        owner_flags(keystore, home),
    ]
    .concat();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let output = tokio::process::Command::from(decdn_command(home, KEYSTORE_PASSWORD)?)
            .args(&args)
            .output()
            .await
            .context("spawn the owner's decdn get")?;
        if output.status.success() {
            return Ok(());
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "the owner's warm-up get never succeeded: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        tokio::time::sleep(Duration::from_millis(750)).await;
    }
}

/// The gateway token, once the gateway has written it and answers on `base`.
async fn wait_for_token(path: &Path, base: &str) -> anyhow::Result<String> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(token) = std::fs::read_to_string(path)
            && reqwest::get(format!("{base}/_capability")).await.is_ok()
        {
            return Ok(token.trim().to_owned());
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "the gateway never came up on {base}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Parse the `{"poolId": "0x…"}` document `decdn pool open` prints.
fn parse_pool_id(stdout: &str) -> anyhow::Result<B256> {
    let parsed: serde_json::Value = serde_json::from_str(stdout.trim())
        .with_context(|| format!("pool open output was not JSON: {stdout:?}"))?;
    let raw = parsed["poolId"]
        .as_str()
        .with_context(|| format!("no poolId in {stdout:?}"))?;
    B256::from_str(raw).context("poolId is not a 32-byte hash")
}
