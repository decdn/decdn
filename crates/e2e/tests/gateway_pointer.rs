//! Live anvil-backed e2e for namespace pointers on a public gateway (#2593).
//!
//! Shape: a node serves a 2 MiB blob and a bundle manifest naming it, a
//! publisher creates a namespace on-chain, and a public gateway pinned to the
//! node runs as a subprocess with an allowance of its own for that namespace.
//!
//!   1. `decdn publish point <ns> decdn:<manifest>/ --gateway …` signs a
//!      pointer with the publisher's keystore; the gateway verifies it against
//!      `PublisherRegistry.ownerOf` and stores it.
//!   2. `Host: <ns>.decdn.test` serves the entry byte-exact.
//!   3. The namespace's own allowance is spent: its host answers `402` naming
//!      `decdn:<manifest>/a.bin?ns=<ns>`, while the bundle's own site, on the
//!      shared allowance, still serves.
//!   4. The same `seq` again is `409`; a pointer signed by a key that does not
//!      own the namespace is `403`.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH` and built `decdn` and `decdn-node` binaries:
//!
//! ```bash
//! cargo build -p decdn-cli -p decdn-node
//! cargo nextest run -p decdn-e2e --features anvil-e2e gateway_pointer
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
use std::time::Duration;

use alloy::primitives::U256;
use anyhow::Context;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::cli::{decdn_command, ensure_decdn_cli_built};
use decdn_e2e::node::NodeFixture;
use decdn_e2e::ports::PortReservation;
use decdn_incentive::eth_identity;
use reqwest::StatusCode;

const MIB: usize = 1024 * 1024;
/// The gateway's working deposit: 10 USDC.
const DEPOSIT_MICRO_USDC: u64 = 10_000_000;
/// The namespace's own allowance: the manifest and one 2 MiB entry fit, a
/// second entry does not.
const NAMESPACE_ALLOWANCE: &str = "3MiB";
const KEYSTORE_PASSWORD: &str = "gateway-pointer-e2e-password";
/// The public origin the gateway serves. Nothing resolves it: every request
/// goes to the loopback listener with this `Host`.
const DOMAIN: &str = "decdn.test";
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
async fn a_namespace_pointer_names_the_bundle_its_host_serves() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("gateway pointer e2e exceeded the overall timeout")??;
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "one sequential journey: node, publisher, gateway, then the requests that depend on \
              all three"
)]
async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();
    ensure_decdn_cli_built()?;
    let chain = ChainFixture::launch().await?;

    // A 2 MiB blob, and a bundle manifest naming it as `a.bin`.
    let a = blob(2 * MIB, 0xA11C_E003);
    let hash_a_hex = blake3::hash(&a).to_hex().to_string();
    let manifest = serde_json::json!({
        "version": 1,
        "entries": [{ "path": "a.bin", "hash": hash_a_hex, "size": a.len() }],
    })
    .to_string()
    .into_bytes();
    let (node, hashes) =
        NodeFixture::launch_with_blobs(&chain, "US", &[a.as_slice(), &manifest]).await?;
    let hash_manifest = hashes[1].to_hex();

    // ---- PUBLISHER: a funded keystore that creates a namespace ----
    let publisher_dir = private_tempdir("publisher")?;
    eth_identity::generate_and_persist(publisher_dir.path(), KEYSTORE_PASSWORD, false)
        .context("generate the publisher keystore")?;
    let publisher = eth_identity::load_signer(
        &eth_identity::keystore_path(publisher_dir.path()),
        KEYSTORE_PASSWORD,
    )?
    .address();
    chain.fund_eth(publisher, 1).await?;
    let registry = format!("{}", chain.addrs().publisher_registry);
    let publish_flags = |home: &Path| {
        [strs(&[
            "--data-dir",
            &home.display().to_string(),
            "--rpc-url",
            &chain.rpc_url(),
            "--chain-id",
            &chain.chain_id().to_string(),
            "--publisher-registry-address",
            &registry,
        ])]
        .concat()
    };
    let created =
        tokio::process::Command::from(decdn_command(publisher_dir.path(), KEYSTORE_PASSWORD)?)
            .args(strs(&["publish", "namespace", "create", "--json"]))
            .args(publish_flags(publisher_dir.path()))
            .output()
            .await
            .context("run decdn publish namespace create")?;
    anyhow::ensure!(
        created.status.success(),
        "namespace create: {}",
        String::from_utf8_lossy(&created.stderr)
    );
    let created: serde_json::Value = serde_json::from_slice(&created.stdout)?;
    let namespace = created["namespace_id"]
        .as_u64()
        .with_context(|| format!("namespace_id in {created}"))?;

    // ---- OPERATOR: a funded keystore the gateway pays from ----
    let gateway_dir = private_tempdir("gateway")?;
    eth_identity::generate_and_persist(gateway_dir.path(), KEYSTORE_PASSWORD, false)
        .context("generate the gateway keystore")?;
    let operator = eth_identity::load_signer(
        &eth_identity::keystore_path(gateway_dir.path()),
        KEYSTORE_PASSWORD,
    )?
    .address();
    chain.fund_eth(operator, 100).await?;
    chain
        .mint_usdc(operator, U256::from(DEPOSIT_MICRO_USDC) * U256::from(4u64))
        .await?;

    // ---- GATEWAY: public, pinned to the node, with the namespace's allowance ----
    // Held until the test ends, so no other process takes the port first.
    let gateway_port = PortReservation::tcp()?;
    let listen = gateway_port.addr().to_string();
    let mut gateway =
        tokio::process::Command::from(decdn_command(gateway_dir.path(), KEYSTORE_PASSWORD)?)
            .args(
                [
                    strs(&[
                        "gateway",
                        "--public-url",
                        &format!("http://{DOMAIN}"),
                        "--listen",
                        &listen,
                        "--data-dir",
                        &gateway_dir.path().display().to_string(),
                        "--working-deposit-micro-usdc",
                        &DEPOSIT_MICRO_USDC.to_string(),
                        "--publisher-registry-address",
                        &registry,
                        "--allowance",
                        &format!("{namespace}={NAMESPACE_ALLOWANCE}"),
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
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    wait_for_gateway(&http, &base).await?;
    let get = |host: &str, path: &str| {
        let request = http.get(format!("{base}{path}")).header("host", host);
        async move {
            let response = request.send().await?;
            let status = response.status();
            let headers = response.headers().clone();
            let body = response.bytes().await?.to_vec();
            anyhow::Ok((status, headers, body))
        }
    };
    let ns_host = format!("{namespace}.{DOMAIN}");

    // ---- (0) Before the pointer, the namespace host is not found ----
    let (status, _, _) = get(&ns_host, "/a.bin").await?;
    anyhow::ensure!(
        status == StatusCode::NOT_FOUND,
        "before the pointer: {status}"
    );

    // ---- (1) `decdn publish point` stores the pointer ----
    let point = |home: &Path, seq: &str| {
        let home = home.to_path_buf();
        let seq = seq.to_owned();
        let uri = format!("decdn:{hash_manifest}/");
        let flags = publish_flags(&home);
        let gateway_url = base.clone();
        async move {
            let output = tokio::process::Command::from(decdn_command(&home, KEYSTORE_PASSWORD)?)
                .args(strs(&[
                    "publish",
                    "point",
                    &namespace.to_string(),
                    &uri,
                    "--gateway",
                    &gateway_url,
                    "--seq",
                    &seq,
                    "--json",
                ]))
                .args(flags)
                .output()
                .await
                .context("run decdn publish point")?;
            anyhow::Ok(output)
        }
    };
    let pointed = point(publisher_dir.path(), "1").await?;
    anyhow::ensure!(
        pointed.status.success(),
        "publish point: {}",
        String::from_utf8_lossy(&pointed.stderr)
    );
    let receipt: serde_json::Value = serde_json::from_slice(&pointed.stdout)?;
    anyhow::ensure!(receipt["status"] == "pointed", "receipt: {receipt}");
    anyhow::ensure!(
        receipt["url"] == format!("http://{ns_host}/"),
        "receipt url: {receipt}"
    );

    // ---- (2) The namespace host serves the entry ----
    // The gateway's first pull opens its pool; the node serves it once its
    // chain watcher has seen the pool, so the first request is retried.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    let got = loop {
        let (status, _, body) = get(&ns_host, "/a.bin").await?;
        if status.is_success() {
            break body;
        }
        anyhow::ensure!(
            status != StatusCode::PAYMENT_REQUIRED,
            "a failed attempt spent the namespace's allowance: {}",
            String::from_utf8_lossy(&body)
        );
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "the namespace host never served its entry: {status} {}",
            String::from_utf8_lossy(&body)
        );
        tokio::time::sleep(Duration::from_millis(750)).await;
    };
    anyhow::ensure!(
        got == a,
        "a.bin: got {} bytes, expected byte-exact",
        got.len()
    );

    // ---- (3) The namespace's own allowance is spent; the shared one is not ----
    let (status, headers, body) = get(&ns_host, "/a.bin").await?;
    anyhow::ensure!(
        status == StatusCode::PAYMENT_REQUIRED,
        "past the namespace allowance: {status} {}",
        String::from_utf8_lossy(&body)
    );
    let uri = format!("decdn:{hash_manifest}/a.bin?ns={namespace}");
    anyhow::ensure!(
        headers.get("link").and_then(|v| v.to_str().ok())
            == Some(format!("<{uri}>; rel=\"alternate\"").as_str()),
        "402 Link: {:?}",
        headers.get("link")
    );
    let (status, headers, _) = get(DOMAIN, &format!("/{hash_manifest}/a.bin")).await?;
    anyhow::ensure!(
        status == StatusCode::FOUND,
        "bundle entry on the bare host: {status}"
    );
    let site = headers
        .get("location")
        .and_then(|v| v.to_str().ok())
        .and_then(|l| l.strip_prefix("http://"))
        .and_then(|rest| rest.strip_suffix("/a.bin"))
        .context("the redirect names the site")?
        .to_owned();
    let (status, _, got) = get(&site, "/a.bin").await?;
    anyhow::ensure!(
        status == StatusCode::OK && got == a,
        "the bundle's own site still serves: {status}, {} bytes",
        got.len()
    );

    // ---- (4) A replayed seq is 409; a stranger's signature is 403 ----
    let replayed = point(publisher_dir.path(), "1").await?;
    anyhow::ensure!(!replayed.status.success(), "the same seq was accepted");
    let stderr = String::from_utf8_lossy(&replayed.stderr);
    anyhow::ensure!(stderr.contains("409"), "replay: {stderr}");
    let stranger = point(gateway_dir.path(), "2").await?;
    anyhow::ensure!(
        !stranger.status.success(),
        "a stranger's pointer was accepted"
    );
    let stderr = String::from_utf8_lossy(&stranger.stderr);
    anyhow::ensure!(stderr.contains("403"), "stranger: {stderr}");

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

/// Wait until the gateway answers its landing page on `base`.
async fn wait_for_gateway(http: &reqwest::Client, base: &str) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(response) = http
            .get(format!("{base}/"))
            .header("host", DOMAIN)
            .send()
            .await
            && response.status() == StatusCode::OK
        {
            return Ok(());
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "the gateway never came up on {base}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
