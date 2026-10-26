//! Live anvil-backed contract run of the TypeScript SDK
//! ([decdn/sdk-typescript](https://github.com/decdn/sdk-typescript)) against
//! a real `decdn gateway`.
//!
//! The SDK's contract suite (`test/contract/*.test.ts` in that repository)
//! runs against its fake gateway in the SDK's own CI. Here the same files run
//! against the binary, so a drift between the fake and the gateway fails:
//!
//!   1. Public: a node serves a blob and a bundle (a text entry, a page that
//!      links the blob, a 256 KiB entry), a publisher owns a namespace, and a
//!      public gateway pinned to the node runs as a subprocess. The suite
//!      fetches and verifies blobs and entries, ranges, `HEAD`, the manifest,
//!      CORS answers, and sets a pointer.
//!   2. Loopback: an owner opens a pool, the gateway mints a delegate and the
//!      owner issues it a capability (`decdn pool assign`). The suite is
//!      refused before it registers the capability, registers and lists it,
//!      then fetches and verifies content with the token.
//!
//! The harness writes what the suite needs (URLs, token, hashes, bytes, the
//! namespace key) to a JSON file named by `DECDN_CONTRACT` and runs vitest
//! with `node`. Every request the suite sends goes to the gateway's loopback
//! listener with the URL's host as `Host`, as behind a TLS proxy.
//!
//! Gated behind the `anvil-e2e` and `sdk-e2e` features (off by default).
//! Requires `anvil` + `forge` + `node` on `PATH`, built `decdn` and
//! `decdn-node` binaries, and `DECDN_SDK_DIR` naming, by absolute path, a
//! checkout of decdn/sdk-typescript with its dependencies installed:
//!
//! ```bash
//! git clone https://github.com/decdn/sdk-typescript ../sdk-typescript
//! pnpm -C ../sdk-typescript install --frozen-lockfile
//! cargo build -p decdn-cli -p decdn-node
//! DECDN_SDK_DIR="$PWD/../sdk-typescript" \
//!   cargo nextest run -p decdn-e2e --features anvil-e2e,sdk-e2e --test gateway_sdk
//! ```

#![cfg(all(feature = "anvil-e2e", feature = "sdk-e2e"))]
// Test scaffolding legitimately uses unwrap/expect/panic; the workspace
// anti-panic policy targets runtime code.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::similar_names
)]

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use alloy::primitives::{B256, U256};
use anyhow::Context;
use decdn_cache::Hash;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::cli::{decdn_command, ensure_decdn_cli_built};
use decdn_e2e::node::NodeFixture;
use decdn_incentive::eth_identity;
use reqwest::StatusCode;

/// The gateway's or the owner's pool deposit: 10 USDC.
const DEPOSIT_MICRO_USDC: u64 = 10_000_000;
/// The delegate's cap: 5 USDC, far above what the suite pulls.
const CAP_MICRO_USDC: u64 = 5_000_000;
const KEYSTORE_PASSWORD: &str = "gateway-sdk-e2e-password";
/// The public origin the gateway serves. Nothing resolves it: the suite sends
/// every request to the loopback listener with this `Host`.
const DOMAIN: &str = "decdn.test";
/// The page host whose capability pays on the loopback gateway.
const PAGE_HOST: &str = "example.com";
/// Heavy journey tier: a 90s warm-up and a 60s start-up ladder, then a
/// vitest run (see [`decdn_e2e::timeout`] for the tier rule).
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::HEAVY;

/// Deterministic pseudo-random bytes (xorshift32), distinct per `seed`.
fn bytes(len: usize, seed: u32) -> Vec<u8> {
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

/// The content both runs serve: a raw blob and a bundle of three entries.
struct Content {
    blob: Vec<u8>,
    files: Vec<(&'static str, Vec<u8>)>,
    manifest: Vec<u8>,
}

impl Content {
    fn new() -> Self {
        let blob = bytes(64 * 1024, 0x0B10_B001);
        let blob_hash = blake3::hash(&blob).to_hex().to_string();
        let files = vec![
            ("a.txt", b"hello from the sdk contract\n".to_vec()),
            (
                "index.html",
                format!("<a href=\"decdn:{blob_hash}\">blob</a>\n").into_bytes(),
            ),
            ("data.bin", bytes(256 * 1024, 0xDA7A_0002)),
        ];
        let entries: Vec<_> = files
            .iter()
            .map(|(path, bytes)| {
                serde_json::json!({
                    "path": path,
                    "hash": format!("b3:{}", blake3::hash(bytes).to_hex()),
                    "size": bytes.len(),
                })
            })
            .collect();
        let manifest = serde_json::json!({ "version": 1, "entries": entries })
            .to_string()
            .into_bytes();
        Self {
            blob,
            files,
            manifest,
        }
    }

    /// Every blob the node serves, the manifest last.
    fn blobs(&self) -> Vec<&[u8]> {
        let mut out: Vec<&[u8]> = vec![self.blob.as_slice()];
        out.extend(self.files.iter().map(|(_, b)| b.as_slice()));
        out.push(self.manifest.as_slice());
        out
    }

    /// The fixture fields that name the content, hex-encoded.
    fn fixture(&self) -> serde_json::Value {
        let files: serde_json::Map<_, _> = self
            .files
            .iter()
            .map(|(path, bytes)| ((*path).to_owned(), alloy::hex::encode(bytes).into()))
            .collect();
        serde_json::json!({
            "bundle": blake3::hash(&self.manifest).to_hex().to_string(),
            "files": files,
            "blob": blake3::hash(&self.blob).to_hex().to_string(),
            "blobBytes": alloy::hex::encode(&self.blob),
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_sdk_contract_holds_on_a_public_gateway() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(public()))
        .await
        .context("gateway sdk public e2e exceeded the overall timeout")??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn the_sdk_contract_holds_on_a_loopback_gateway() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(loopback()))
        .await
        .context("gateway sdk loopback e2e exceeded the overall timeout")??;
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "one sequential setup: node, publisher, operator, gateway, then the suite"
)]
async fn public() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();
    let sdk = sdk_dir()?;
    ensure_decdn_cli_built()?;
    let chain = ChainFixture::launch().await?;
    let content = Content::new();
    let (node, hashes) = NodeFixture::launch_with_blobs(&chain, "US", &content.blobs()).await?;
    let blob_hash = hashes[0];

    // ---- PUBLISHER: a namespace its key owns ----
    let publisher_dir = private_tempdir("publisher")?;
    eth_identity::generate_and_persist(publisher_dir.path(), KEYSTORE_PASSWORD, false)
        .context("generate the publisher keystore")?;
    let publisher = eth_identity::load_signer(
        &eth_identity::keystore_path(publisher_dir.path()),
        KEYSTORE_PASSWORD,
    )?;
    chain.fund_eth(publisher.address(), 1).await?;
    let registry = format!("{}", chain.addrs().publisher_registry);
    let created = decdn_ok(
        publisher_dir.path(),
        &strs(&[
            "publish",
            "namespace",
            "create",
            "--json",
            "--data-dir",
            &publisher_dir.path().display().to_string(),
            "--rpc-url",
            &chain.rpc_url(),
            "--chain-id",
            &chain.chain_id().to_string(),
            "--publisher-registry-address",
            &registry,
        ]),
    )
    .await?;
    let created: serde_json::Value = serde_json::from_str(created.trim())?;
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

    // ---- GATEWAY: public, pinned to the node ----
    let listen = format!("127.0.0.1:{}", free_port()?);
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
                        "--allowance-bytes-per-day",
                        "1GiB",
                        "--publisher-registry-address",
                        &registry,
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
    let http = reqwest::Client::new();
    // The gateway's first pull opens its pool, and the node serves it once
    // its chain watcher has seen the pool: the suite starts after that.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let status = http
            .get(format!("{base}/{}", blob_hash.to_hex()))
            .header("host", DOMAIN)
            .send()
            .await
            .map(|r| r.status());
        if matches!(status, Ok(StatusCode::OK)) {
            break;
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "the gateway never served the blob: {status:?}"
        );
        tokio::time::sleep(Duration::from_millis(750)).await;
    }

    let mut fixture = content.fixture();
    fixture["mode"] = "public".into();
    fixture["gatewayUrl"] = format!("http://{DOMAIN}").into();
    fixture["connect"] = listen.clone().into();
    fixture["pointer"] = serde_json::json!({
        "chainId": chain.chain_id().to_string(),
        "publisherRegistry": registry,
        "namespace": namespace.to_string(),
        "privateKey": format!("{}", publisher.to_bytes()),
    });
    let result = run_suite(&sdk, "test/contract/public.test.ts", &fixture).await;
    let _ = gateway.kill().await;
    result
}

#[allow(
    clippy::too_many_lines,
    reason = "one sequential setup: node, owner, gateway, capability, then the suite"
)]
async fn loopback() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();
    let sdk = sdk_dir()?;
    ensure_decdn_cli_built()?;
    let chain = ChainFixture::launch().await?;
    let content = Content::new();
    let warm = bytes(64 * 1024, 0x0C0F_FEE3);
    let mut blobs = content.blobs();
    blobs.push(&warm);
    let (node, hashes) = NodeFixture::launch_with_blobs(&chain, "US", &blobs).await?;
    let hash_warm = hashes[hashes.len() - 1];

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
    // The owner's own fetch waits out the node's view of the fresh pool, so
    // the gateway never takes that race for a funding stop.
    warm_up(&chain, &node, owner_dir.path(), &owner_keystore, &hash_warm).await?;

    // ---- GATEWAY: loopback, pinned to the node ----
    let gateway_dir = private_tempdir("gateway")?;
    let listen = format!("127.0.0.1:{}", free_port()?);
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

    // ---- A delegate the gateway holds, and a capability the owner issues it ----
    let minted: serde_json::Value = reqwest::Client::new()
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

    let mut fixture = content.fixture();
    fixture["mode"] = "loopback".into();
    fixture["gatewayUrl"] = base.clone().into();
    fixture["connect"] = listen.clone().into();
    fixture["token"] = token.into();
    fixture["host"] = PAGE_HOST.into();
    fixture["capability"] = capability.into();
    let result = run_suite(&sdk, "test/contract/loopback.test.ts", &fixture).await;
    let _ = gateway.kill().await;
    result
}

/// The SDK checkout `DECDN_SDK_DIR` names, with its dependencies installed. A
/// missing variable, `node` or `node_modules` fails the run: a skipped
/// contract run would hide drift.
fn sdk_dir() -> anyhow::Result<PathBuf> {
    let sdk = std::env::var_os("DECDN_SDK_DIR")
        .filter(|dir| !dir.is_empty())
        .map(PathBuf::from)
        .context(
            "DECDN_SDK_DIR is not set: point it at a checkout of \
             https://github.com/decdn/sdk-typescript",
        )?;
    // nextest runs a test in its crate's directory, so a relative path would
    // name something other than what the caller meant.
    anyhow::ensure!(
        sdk.is_absolute(),
        "DECDN_SDK_DIR must be an absolute path, got {}",
        sdk.display()
    );
    let vitest = sdk.join("node_modules/vitest/vitest.mjs");
    anyhow::ensure!(
        vitest.is_file(),
        "{} is missing: run `pnpm -C {} install --frozen-lockfile` first",
        vitest.display(),
        sdk.display()
    );
    let node = std::process::Command::new("node")
        .arg("--version")
        .output()
        .context("`node` is not on PATH: the SDK contract run needs Node.js 20.19+")?;
    anyhow::ensure!(node.status.success(), "`node --version` failed");
    Ok(sdk)
}

/// Run the contract suite `file` in `sdk` against `fixture`, requiring it to
/// pass. Its output goes to this test's.
async fn run_suite(sdk: &Path, file: &str, fixture: &serde_json::Value) -> anyhow::Result<()> {
    let dir = tempfile::tempdir().context("fixture tempdir")?;
    let path = dir.path().join("contract.json");
    std::fs::write(&path, serde_json::to_vec_pretty(fixture)?).context("write the fixture")?;
    let status = tokio::process::Command::new("node")
        .current_dir(sdk)
        .args([
            "node_modules/vitest/vitest.mjs",
            "run",
            "--project",
            "contract",
            file,
        ])
        .env("DECDN_CONTRACT", &path)
        .status()
        .await
        .context("spawn node vitest")?;
    anyhow::ensure!(status.success(), "the SDK contract suite failed: {status}");
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

/// A loopback port nothing listens on.
fn free_port() -> anyhow::Result<u16> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
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

/// Run `decdn <args>` from `home`, requiring success, and return its stdout.
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
