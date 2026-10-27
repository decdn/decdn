//! Live anvil-backed e2e for `decdn gateway --public-url` (#2593).
//!
//! Shape: a node serves two blobs and a bundle manifest naming one of them,
//! and a public gateway pinned to that node runs as a subprocess, paying
//! from an operator keystore's own pool. With no token and no browser
//! headers:
//!
//!   1. The bundle's site serves its entry byte-exact, `HEAD` answers the
//!      entry's length and hash from the manifest, a `Range` is served as
//!      206 over exactly that window, and a range past the end is 416. (That
//!      the pull pays for the aligned span alone is the client's unit tests'
//!      to pin down.)
//!   2. The bare host streams a blob.
//!   3. Four pulls run at once on the one pool payer, and the node rejects
//!      none of their vouchers.
//!   4. The entry no longer fits the day's allowance: `402` naming its
//!      `decdn:` URI, before any pull.
//!   5. An unknown `Host` gets `403`.
//!   6. The metrics listener on loopback counts what the gateway served, and
//!      the node wrote the wire bytes of those pulls and no more: no pull of
//!      a.bin or b opens a stream it drops unread (#2619). Every pull reaches
//!      the node, since the gateway keeps no blob bodies. Its `/_status`
//!      agrees with the exposition and shows the pool's lane and the spent
//!      allowance; its `/` is no page.
//!   7. `decdn dashboard`, pointed at that listener and at the node, shows the
//!      gateway's state and the node's health and lanes.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH` and built `decdn` and `decdn-node` binaries:
//!
//! ```bash
//! cargo build -p decdn-cli -p decdn-node
//! cargo nextest run -p decdn-e2e --features anvil-e2e gateway_public
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

use std::time::Duration;

use alloy::primitives::U256;
use anyhow::Context;
use decdn_cache::Hash;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::cli::{decdn_command, ensure_decdn_cli_built};
use decdn_e2e::node::NodeFixture;
use decdn_incentive::eth_identity;
use reqwest::StatusCode;

const MIB: usize = 1024 * 1024;
/// The gateway's working deposit: 10 USDC.
const DEPOSIT_MICRO_USDC: u64 = 10_000_000;
/// The day's allowance: six 4 MiB pulls, a 1.5 MiB range and the manifest
/// fit, a further 4 MiB entry does not.
const ALLOWANCE: &str = "27MiB";
const KEYSTORE_PASSWORD: &str = "gateway-public-e2e-password";
/// The public origin the gateway serves. Nothing resolves it: every request
/// goes to the loopback listener with this `Host`.
const DOMAIN: &str = "decdn.test";
/// The node's count of paying streams it stopped over a rejected voucher.
const VOUCHER_REJECTED: &str = "decdn_serve_stream_voucher_rejected_total";
/// The bao wire bytes the node wrote on `cdn/client/v1`.
const SERVED: &str = "decdn_bytes_served_total";
/// What the node may write past the wire bytes of the pulls: the parent
/// nodes a pull's later legs repeat, a few hundred bytes here. For the 4 MiB
/// blobs here, below the 64 KiB first frame that a stream opened and dropped
/// unread costs at the least.
const LEG_OVERHEAD_SLACK: u64 = 16 * 1024;
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
async fn a_public_gateway_serves_anyone_from_its_own_pool() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("gateway public e2e exceeded the overall timeout")??;
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    clippy::cognitive_complexity,
    reason = "one sequential journey: node, operator, gateway, then the requests that depend on \
              all three"
)]
async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();
    ensure_decdn_cli_built()?;
    let chain = ChainFixture::launch().await?;

    // Two 4 MiB blobs, and a bundle manifest naming the first as `a.bin`.
    let a = blob(4 * MIB, 0xA11C_E001);
    let b = blob(4 * MIB, 0xB0B0_0002);
    let hash_a_hex = blake3::hash(&a).to_hex().to_string();
    let manifest = serde_json::json!({
        "version": 1,
        "entries": [{ "path": "a.bin", "hash": hash_a_hex, "size": a.len() }],
    })
    .to_string()
    .into_bytes();
    let (node, hashes) =
        NodeFixture::launch_with_blobs(&chain, "US", &[a.as_slice(), b.as_slice(), &manifest])
            .await?;
    let (hash_a, hash_b, hash_manifest) = (hashes[0], hashes[1], hashes[2]);
    anyhow::ensure!(
        hash_a.to_hex() == hash_a_hex,
        "the node hashes as the test does"
    );

    // ---- OPERATOR: a funded keystore the gateway pays from ----
    let gateway_dir = private_tempdir("gateway")?;
    eth_identity::generate_and_persist(gateway_dir.path(), KEYSTORE_PASSWORD, false)
        .context("generate the gateway keystore")?;
    let keystore = eth_identity::keystore_path(gateway_dir.path());
    let operator = eth_identity::load_signer(&keystore, KEYSTORE_PASSWORD)?.address();
    chain.fund_eth(operator, 100).await?;
    chain
        .mint_usdc(operator, U256::from(DEPOSIT_MICRO_USDC) * U256::from(4u64))
        .await?;

    // ---- GATEWAY: public, pinned to the node, on a free loopback port ----
    let port = free_port()?;
    let listen = format!("127.0.0.1:{port}");
    let metrics = format!("127.0.0.1:{}", free_port()?);
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
                        ALLOWANCE,
                        "--metrics-listen",
                        &metrics,
                        // A public gateway verifies namespace pointers, and
                        // anvil is no chain the CLI knows a registry for.
                        "--publisher-registry-address",
                        &format!("{}", chain.addrs().publisher_registry),
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
    // Nothing resolves the site hosts a redirect names: the test follows
    // them by hand, with the `Host` header.
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    wait_for_gateway(&http, &base).await?;
    let served_before = node.scrape_metric(SERVED).await?;
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

    // ---- (1) The bundle's site, found through the bare host's redirect ----
    let (status, headers, _) = get(DOMAIN, &format!("/{}/a.bin", hash_manifest.to_hex())).await?;
    anyhow::ensure!(
        status == StatusCode::FOUND,
        "bundle entry on the bare host: {status}"
    );
    let location = headers
        .get("location")
        .and_then(|v| v.to_str().ok())
        .context("the redirect names the site")?;
    let site = location
        .strip_prefix("http://")
        .and_then(|rest| rest.strip_suffix("/a.bin"))
        .with_context(|| format!("site redirect {location:?}"))?
        .to_owned();
    anyhow::ensure!(site.ends_with(&format!(".{DOMAIN}")), "{site}");

    // The gateway's first pull opens its pool; the node serves it once its
    // chain watcher has seen the pool, so the first request is retried.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    let got = loop {
        let (status, _, body) = get(&site, "/a.bin").await?;
        if status.is_success() {
            break body;
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "the site never served its entry: {status} {}",
            String::from_utf8_lossy(&body)
        );
        tokio::time::sleep(Duration::from_millis(750)).await;
    };
    anyhow::ensure!(
        got == a,
        "a.bin: got {} bytes, expected byte-exact",
        got.len()
    );

    let head = http
        .head(format!("{base}/a.bin"))
        .header("host", &site)
        .send()
        .await?;
    anyhow::ensure!(
        head.status() == StatusCode::OK,
        "HEAD a.bin: {}",
        head.status()
    );
    anyhow::ensure!(
        head.headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            == Some(a.len().to_string().as_str()),
        "HEAD a.bin content-length: {:?}",
        head.headers().get("content-length")
    );
    anyhow::ensure!(
        head.headers().get("etag").and_then(|v| v.to_str().ok())
            == Some(format!("\"{hash_a_hex}\"").as_str()),
        "HEAD a.bin etag: {:?}",
        head.headers().get("etag")
    );
    anyhow::ensure!(
        head.headers()
            .get("accept-ranges")
            .and_then(|v| v.to_str().ok())
            == Some("bytes"),
        "HEAD a.bin accept-ranges: {:?}",
        head.headers().get("accept-ranges")
    );

    // A range off the chunk-group grid: the response is exactly the window.
    let (first, last) = (1_048_577usize, 2_621_440usize);
    let ranged = http
        .get(format!("{base}/a.bin"))
        .header("host", &site)
        .header("range", format!("bytes={first}-{last}"))
        .send()
        .await?;
    anyhow::ensure!(
        ranged.status() == StatusCode::PARTIAL_CONTENT,
        "range: {}",
        ranged.status()
    );
    anyhow::ensure!(
        ranged
            .headers()
            .get("content-range")
            .and_then(|v| v.to_str().ok())
            == Some(format!("bytes {first}-{last}/{}", a.len()).as_str()),
        "content-range: {:?}",
        ranged.headers().get("content-range")
    );
    let window = ranged.bytes().await?;
    anyhow::ensure!(
        window.as_ref() == &a[first..=last],
        "range: got {} bytes, expected the {}-byte window byte-exact",
        window.len(),
        last - first + 1
    );
    let past = http
        .get(format!("{base}/a.bin"))
        .header("host", &site)
        .header("range", format!("bytes={}-", a.len()))
        .send()
        .await?;
    anyhow::ensure!(
        past.status() == StatusCode::RANGE_NOT_SATISFIABLE,
        "range past the end: {}",
        past.status()
    );

    // ---- (2) A blob from the bare host ----
    let (status, _, got) = get(DOMAIN, &format!("/{}", hash_b.to_hex())).await?;
    anyhow::ensure!(status == StatusCode::OK, "blob b: {status}");
    anyhow::ensure!(got == b, "blob b: got {} bytes", got.len());

    // ---- (3) Four pulls at once on the one pool payer ----
    let rejected_before = node.scrape_metric(VOUCHER_REJECTED).await?;
    let paid = |hash: Hash| {
        let request = get(DOMAIN, &format!("/{}", hash.to_hex()));
        async move {
            let (status, _, body) = request.await?;
            anyhow::ensure!(
                status.is_success(),
                "{status}: {}",
                String::from_utf8_lossy(&body)
            );
            anyhow::Ok(body)
        }
    };
    let (got_a1, got_b1, got_a2, got_b2) =
        tokio::try_join!(paid(hash_a), paid(hash_b), paid(hash_a), paid(hash_b))?;
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
        "the node rejected {rejected} voucher stream(s) on the pool payer's lane"
    );

    // ---- (4) The entry no longer fits the day's allowance ----
    let (status, headers, body) = get(&site, "/a.bin").await?;
    anyhow::ensure!(
        status == StatusCode::PAYMENT_REQUIRED,
        "past the allowance: {status} {}",
        String::from_utf8_lossy(&body)
    );
    let uri = format!("decdn:{}/a.bin", hash_manifest.to_hex());
    anyhow::ensure!(
        headers.get("link").and_then(|v| v.to_str().ok())
            == Some(format!("<{uri}>; rel=\"alternate\"").as_str()),
        "402 Link: {:?}",
        headers.get("link")
    );
    let refused: serde_json::Value = serde_json::from_slice(&body)?;
    anyhow::ensure!(refused["uri"] == uri, "402 body: {refused}");

    // ---- (5) An unknown host ----
    let (status, _, _) = get("evil.test", "/").await?;
    anyhow::ensure!(status == StatusCode::FORBIDDEN, "evil.test: {status}");

    // ---- (6) The metrics listener ----
    let exposition = http
        .get(format!("http://{metrics}/metrics"))
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    let counter = |name: &str| {
        exposition
            .lines()
            .find_map(|l| l.strip_prefix(&format!("{name} ")))
            .and_then(|n| n.parse::<u64>().ok())
            .with_context(|| format!("{name} in the exposition"))
    };
    anyhow::ensure!(
        counter("decdn_gateway_bytes_served_total")? >= 6 * a.len() as u64,
        "bytes served: {exposition}"
    );
    // a.bin three times, b three times, the range and the manifest.
    let pulled = 3 * wire_bytes(0, 0, a.len())?
        + 3 * wire_bytes(0, 0, b.len())?
        + wire_bytes(first, last - first + 1, a.len())?
        + wire_bytes(0, 0, manifest.len())?;
    let served = node
        .scrape_metric(SERVED)
        .await?
        .saturating_sub(served_before);
    anyhow::ensure!(
        (pulled..=pulled + LEG_OVERHEAD_SLACK).contains(&served),
        "the node wrote {served} bytes for {pulled} wire bytes of pulls"
    );
    anyhow::ensure!(
        counter("decdn_gateway_requests_allowance_exhausted_total")? == 1,
        "402s: {exposition}"
    );
    anyhow::ensure!(
        counter("decdn_gateway_requests_refused_total")? >= 2,
        "refused (416 and 403): {exposition}"
    );
    anyhow::ensure!(
        counter("decdn_gateway_pulls_active")? == 0,
        "pulls in flight: {exposition}"
    );

    // The same counters, and what the store holds, as `/_status` has them.
    let status: serde_json::Value = serde_json::from_str(
        &http
            .get(format!("http://{metrics}/_status"))
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?,
    )?;
    anyhow::ensure!(status["mode"] == "public", "mode: {status}");
    let counters = &status["counters"];
    anyhow::ensure!(
        counters["bytes_served"].as_u64() == Some(counter("decdn_gateway_bytes_served_total")?),
        "bytes served disagree: {counters} vs {exposition}"
    );
    anyhow::ensure!(
        counters["requests_allowance_exhausted"] == 1 && counters["pulls_active"] == 0,
        "counters: {counters}"
    );
    let pools = status["pools"]["pools"]["pools"]
        .as_array()
        .with_context(|| format!("pools: {status}"))?;
    anyhow::ensure!(pools.len() == 1, "one pool: {status}");
    let lanes = pools[0]["lanes"].as_array().context("lanes")?;
    anyhow::ensure!(
        lanes
            .iter()
            .any(|l| l["last_bytes_delivered"].as_u64().is_some_and(|b| b > 0)),
        "a lane that delivered: {status}"
    );
    anyhow::ensure!(
        status["allowance"][0]["used"]
            .as_u64()
            .is_some_and(|u| u > 0),
        "the shared bucket spent: {status}"
    );
    let page = http.get(format!("http://{metrics}/")).send().await?;
    anyhow::ensure!(
        page.status() == StatusCode::NOT_FOUND,
        "the gateway serves no page: {}",
        page.status()
    );

    // ---- (7) `decdn dashboard` shows the gateway and the node ----
    let dashboard_dir = private_tempdir("dashboard")?;
    let dashboard_listen = format!("127.0.0.1:{}", free_port()?);
    let mut dashboard =
        tokio::process::Command::from(decdn_command(dashboard_dir.path(), KEYSTORE_PASSWORD)?)
            .args(strs(&[
                "dashboard",
                "--listen",
                &dashboard_listen,
                "--gateway",
                &format!("http://{metrics}"),
                "--admin-url",
                node.admin_url(),
                "--metrics-url",
                &node.metrics_url(),
            ]))
            .kill_on_drop(true)
            .spawn()
            .context("spawn decdn dashboard")?;
    let token_file = dashboard_dir.path().join(".decdn/dashboard/token");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let shown = loop {
        let token = std::fs::read_to_string(&token_file).unwrap_or_default();
        let answer = http
            .get(format!("http://{dashboard_listen}/_status"))
            .header("x-decdn-token", token.trim())
            .send()
            .await;
        if let Ok(answer) = answer
            && answer.status() == StatusCode::OK
        {
            let shown: serde_json::Value = answer.json().await?;
            if shown["gateway"]["state"] == "up"
                && shown["node"]["state"] == "up"
                && shown["node"]["lanes"]["state"] == "ok"
                && shown["node"]["metrics"]["state"] == "ok"
            {
                break shown;
            }
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "the dashboard never showed the gateway and the node"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    anyhow::ensure!(
        shown["gateway"]["status"]["mode"] == "public",
        "the gateway's own /_status: {shown}"
    );
    anyhow::ensure!(
        shown["node"]["lanes"]["value"]["lanes"]
            .as_array()
            .is_some_and(|l| !l.is_empty()),
        "the node's lane for the gateway's pool: {shown}"
    );
    let unauthorized = http
        .get(format!("http://{dashboard_listen}/_status"))
        .send()
        .await?;
    anyhow::ensure!(
        unauthorized.status() == StatusCode::UNAUTHORIZED,
        "the dashboard's /_status takes its token: {}",
        unauthorized.status()
    );
    dashboard.start_kill().context("stop the dashboard")?;
    let _ = dashboard.wait().await;

    gateway.start_kill().context("stop the gateway")?;
    let _ = gateway.wait().await;
    drop(node);
    Ok(())
}

/// The bytes a pull of `len` bytes at `offset` of a `total`-byte blob puts on
/// the wire: its chunk-group aligned content and the interleaved bao proof
/// nodes. `(0, 0)` is the whole blob.
fn wire_bytes(offset: usize, len: usize, total: usize) -> anyhow::Result<u64> {
    Ok(decdn_bao_range::align_range_clamped(offset as u64, len as u64, total as u64)?.wire_len())
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
