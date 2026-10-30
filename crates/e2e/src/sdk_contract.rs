//! The SDK contract run: an SDK's contract suite against a real
//! `decdn gateway`, on an anvil chain.
//!
//! An SDK's contract suite runs against its fake gateway in the SDK's own CI.
//! Here the same suite runs against the binary, so a drift between the fake
//! and the gateway fails. The harness sets up what each mode needs; what the
//! suite checks is the suite's:
//!
//!   1. [`Mode::Public`]: a node serves a blob and a bundle (a text entry, a
//!      page that links the blob, a 256 KiB entry), a publisher owns two
//!      namespaces, and a public gateway pinned to the node runs as a
//!      subprocess.
//!   2. [`Mode::Loopback`]: an owner opens a pool, the gateway mints two
//!      delegates, and the owner issues each a capability (`decdn pool
//!      assign`) for its own page host, which the suite registers.
//!
//! Each client flavour of a suite (see [`Suite::flavours`]) gets its own
//! namespace or capability, so no flavour sees another flavour's state. The
//! fixture names all of them (`pointer.namespaces`, `capabilities`); its
//! single-value fields (`pointer.namespace`, `host`, `capability`) name the
//! first.
//!
//! The harness writes what the suite needs (URLs, token, hashes, bytes, the
//! namespace key) to a JSON file and gives its path to [`Suite::run`]. The
//! suite connects every request to the gateway's listener and sends the URL's
//! host as `Host`; on the public run this stands in for the TLS proxy in front
//! of `http://decdn.test`. A run passes only when every collected test passed,
//! and when the gateway holds what only the suite's own requests create (the
//! namespace pointer, the registered capability), so neither a skipped suite
//! nor one that fell back to the fake passes.
//!
//! A [`Suite`] names one SDK's suite and how to run it; [`TypeScript`] runs
//! [decdn/sdk-typescript](https://github.com/decdn/sdk-typescript)'s with
//! vitest, and [`Python`] runs
//! [decdn/sdk-python](https://github.com/decdn/sdk-python)'s with pytest.
//! [`run`] drives one mode end to end. A run requires `anvil` +
//! `forge` on `PATH`, built `decdn` and `decdn-node` binaries, and what the
//! suite's own constructor checks.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use alloy::primitives::{B256, U256};
use anyhow::Context;
use decdn_cache::Hash;
use decdn_incentive::eth_identity;
use reqwest::StatusCode;

use crate::chain::ChainFixture;
use crate::cli::{decdn_command, ensure_decdn_cli_built};
use crate::node::NodeFixture;
use crate::ports::PortReservation;

mod junit;

/// The gateway's or the owner's pool deposit: 10 USDC.
const DEPOSIT_MICRO_USDC: u64 = 10_000_000;
/// The delegate's cap: 5 USDC, far above what the suite pulls.
const CAP_MICRO_USDC: u64 = 5_000_000;
const KEYSTORE_PASSWORD: &str = "gateway-sdk-e2e-password";
/// The public origin the gateway serves. Nothing resolves it: the suite sends
/// every request to the loopback listener with this `Host`.
const DOMAIN: &str = "decdn.test";
/// The most client flavours a suite can run: the harness sets up one
/// namespace (public) or one capability (loopback) per flavour.
const MAX_FLAVOURS: usize = 2;
/// The page hosts whose capabilities pay on the loopback gateway, one per
/// client flavour, the first flavour's first.
const PAGE_HOSTS: [&str; MAX_FLAVOURS] = ["example.com", "example.net"];
/// The longest one harness request may take: a stuck request fails here, not
/// at the overall timeout.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// Heavy journey tier (see [`crate::timeout`] for the tier rule): the
/// loopback run polls up to 90s (the owner's warm-up) and 60s (the token),
/// the public run up to 90s (the first pull), and each then runs the suite.
const OVERALL_TIMEOUT: Duration = crate::timeout::HEAVY;

/// The gateway a contract run sets up, and the suite file that runs against
/// it.
#[derive(Debug, Clone, Copy)]
pub enum Mode {
    /// A public gateway that serves `http://decdn.test` and its namespace
    /// hosts, and pays from its operator's pool.
    Public,
    /// A loopback gateway that holds a delegate and pays from the owner's
    /// pool through the capability the suite registers.
    Loopback,
}

impl Mode {
    /// The mode's name in messages and in the fixture's `mode` field.
    const fn label(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Loopback => "loopback",
        }
    }
}

/// The counts a suite run reports.
#[derive(Debug, Clone, Copy)]
pub struct Outcome {
    /// The tests the suite collected.
    pub total: u64,
    /// The tests that passed. A skipped or todo test does not count.
    pub passed: u64,
}

/// One SDK's contract suite, and how to run it against a fixture file.
pub trait Suite: Send + Sync {
    /// The SDK's name in messages.
    fn name(&self) -> &'static str;

    /// How many independent client flavours the suite runs against one
    /// gateway (1 for TypeScript, at most 2). Flavour `i` uses the fixture's
    /// `i`-th namespace or capability, and the harness checks that the suite
    /// created the gateway state of each flavour it runs.
    fn flavours(&self) -> usize;

    /// Run the suite file for `mode` against the fixture at `fixture`, and
    /// report its counts. `scratch` is a directory the run can write to (a
    /// report file, for example). A suite that fails returns an error; the
    /// caller checks the counts.
    fn run(
        &self,
        mode: Mode,
        fixture: &Path,
        scratch: &Path,
    ) -> impl Future<Output = anyhow::Result<Outcome>> + Send;
}

/// Run `suite`'s contract file for `mode` against a real gateway, within the
/// heavy journey tier.
///
/// # Errors
///
/// Fails when the suite runs no client flavour or more than the harness sets
/// up for, the setup fails, the suite fails or skips a test, the gateway does
/// not hold what the suite creates, or the run exceeds the tier.
pub async fn run<S: Suite>(suite: &S, mode: Mode) -> anyhow::Result<()> {
    anyhow::ensure!(
        (1..=MAX_FLAVOURS).contains(&suite.flavours()),
        "the {} SDK runs {} client flavours; the harness sets up 1 to {MAX_FLAVOURS}",
        suite.name(),
        suite.flavours()
    );
    let setup = async {
        match mode {
            Mode::Public => public(suite).await,
            Mode::Loopback => loopback(suite).await,
        }
    };
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(setup))
        .await
        .with_context(|| {
            format!(
                "gateway {} SDK {} e2e exceeded the overall timeout",
                suite.name(),
                mode.label()
            )
        })?
}

/// The TypeScript SDK
/// ([decdn/sdk-typescript](https://github.com/decdn/sdk-typescript)): its
/// `test/contract/*.test.ts` files, run with vitest under `node`.
#[derive(Debug)]
pub struct TypeScript {
    dir: PathBuf,
}

impl TypeScript {
    /// The SDK checkout `DECDN_SDK_DIR` names, with its dependencies
    /// installed. A missing variable, `node` or `node_modules` fails the run:
    /// a skipped contract run would hide drift.
    ///
    /// # Errors
    ///
    /// Fails when `DECDN_SDK_DIR` is unset, empty or relative, when the
    /// checkout has no installed vitest, or when `node` does not run.
    pub fn from_env() -> anyhow::Result<Self> {
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
        Ok(Self { dir: sdk })
    }
}

impl Suite for TypeScript {
    fn name(&self) -> &'static str {
        "TypeScript"
    }

    fn flavours(&self) -> usize {
        1
    }

    /// Run the vitest `contract` project's file for `mode`, with a JSON
    /// report in `scratch`. Its output goes to this test's.
    async fn run(&self, mode: Mode, fixture: &Path, scratch: &Path) -> anyhow::Result<Outcome> {
        let file = match mode {
            Mode::Public => "test/contract/public.test.ts",
            Mode::Loopback => "test/contract/loopback.test.ts",
        };
        let report = scratch.join("report.json");
        let status = tokio::process::Command::new("node")
            .current_dir(&self.dir)
            .args([
                "node_modules/vitest/vitest.mjs",
                "run",
                "--project",
                "contract",
                "--reporter=default",
                "--reporter=json",
                &format!("--outputFile.json={}", report.display()),
                file,
            ])
            .env("DECDN_CONTRACT", fixture)
            // Without the fixture the suite fails instead of testing its fake.
            .env("DECDN_CONTRACT_REQUIRED", "1")
            .kill_on_drop(true)
            .spawn()
            .context("spawn node vitest")?
            .wait()
            .await
            .context("wait for node vitest")?;
        anyhow::ensure!(status.success(), "the SDK contract suite failed: {status}");
        let report: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&report).context("read the vitest JSON report")?)
                .context("parse the vitest JSON report")?;
        let total = report
            .get("numTotalTests")
            .and_then(serde_json::Value::as_u64)
            .context("numTotalTests in the vitest report")?;
        let passed = report
            .get("numPassedTests")
            .and_then(serde_json::Value::as_u64)
            .context("numPassedTests in the vitest report")?;
        Ok(Outcome { total, passed })
    }
}

/// The Python SDK
/// ([decdn/sdk-python](https://github.com/decdn/sdk-python)): its
/// `tests/contract/test_*.py` files, run with pytest under `uv`. Each file
/// runs every test once per client flavour (sync and async).
#[derive(Debug)]
pub struct Python {
    dir: PathBuf,
}

impl Python {
    /// The SDK checkout `DECDN_SDK_PYTHON_DIR` names, checked as
    /// [`Python::from_dir`] checks it. A missing variable fails the run: a
    /// skipped contract run would hide drift.
    ///
    /// # Errors
    ///
    /// Fails when `DECDN_SDK_PYTHON_DIR` is unset or empty, or when
    /// [`Python::from_dir`] fails.
    pub fn from_env() -> anyhow::Result<Self> {
        let sdk = std::env::var_os("DECDN_SDK_PYTHON_DIR")
            .filter(|dir| !dir.is_empty())
            .map(PathBuf::from)
            .context(
                "DECDN_SDK_PYTHON_DIR is not set: point it at a checkout of \
                 https://github.com/decdn/sdk-python",
            )?;
        Self::from_dir(sdk)
    }

    /// The SDK checkout at `dir`, with its virtual environment synced from
    /// its lockfile.
    ///
    /// # Errors
    ///
    /// Fails when `dir` is relative, when it has no `uv.lock` or no synced
    /// `.venv`, or when `uv` does not run.
    pub fn from_dir(dir: PathBuf) -> anyhow::Result<Self> {
        // nextest runs a test in its crate's directory, so a relative path would
        // name something other than what the caller meant.
        anyhow::ensure!(
            dir.is_absolute(),
            "the Python SDK checkout (DECDN_SDK_PYTHON_DIR) must be an absolute path, got {}",
            dir.display()
        );
        let lock = dir.join("uv.lock");
        anyhow::ensure!(
            lock.is_file(),
            "{} is missing: {} is not a checkout of decdn/sdk-python",
            lock.display(),
            dir.display()
        );
        let python = dir.join(".venv/bin/python");
        anyhow::ensure!(
            python.is_file(),
            "{} is missing: run `uv sync --frozen` in {} first",
            python.display(),
            dir.display()
        );
        let uv = std::process::Command::new("uv")
            .arg("--version")
            .output()
            .context("`uv` is not on PATH: the Python SDK contract run needs uv")?;
        anyhow::ensure!(uv.status.success(), "`uv --version` failed");
        Ok(Self { dir })
    }
}

impl Suite for Python {
    fn name(&self) -> &'static str {
        "Python"
    }

    fn flavours(&self) -> usize {
        2
    }

    /// Run the pytest file for `mode` from the synced environment, with a
    /// JUnit report in `scratch`. Its output goes to this test's.
    async fn run(&self, mode: Mode, fixture: &Path, scratch: &Path) -> anyhow::Result<Outcome> {
        let file = match mode {
            Mode::Public => "tests/contract/test_public.py",
            Mode::Loopback => "tests/contract/test_loopback.py",
        };
        let report = scratch.join("report.xml");
        let status = tokio::process::Command::new("uv")
            .current_dir(&self.dir)
            .args(["run", "--frozen", "--no-sync", "pytest", file])
            // No `.pytest_cache` in the checkout.
            .args(["-p", "no:cacheprovider", "--junit-xml"])
            .arg(&report)
            .args(["-o", "junit_family=xunit2"])
            .env("DECDN_CONTRACT", fixture)
            // Without the fixture the suite fails instead of testing its fake.
            .env("DECDN_CONTRACT_REQUIRED", "1")
            .kill_on_drop(true)
            .spawn()
            .context("spawn uv run pytest")?
            .wait()
            .await
            .context("wait for uv run pytest")?;
        // pytest exits 5 when it collects no test, so an empty file fails here.
        anyhow::ensure!(
            status.success(),
            "the Python SDK contract suite failed ({}): {status}",
            mode.label()
        );
        let xml = std::fs::read_to_string(&report).context("read the pytest JUnit report")?;
        let counts = junit::counts(&xml).context("parse the pytest JUnit report")?;
        Ok(Outcome {
            total: counts.tests,
            passed: counts.passed(),
        })
    }
}

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

    /// The content's blobs, the manifest last.
    fn blobs(&self) -> Vec<&[u8]> {
        let mut out: Vec<&[u8]> = vec![self.blob.as_slice()];
        out.extend(self.files.iter().map(|(_, b)| b.as_slice()));
        out.push(self.manifest.as_slice());
        out
    }

    /// The bytes of the bundle file at `path`.
    fn file(&self, path: &str) -> Option<&[u8]> {
        self.files
            .iter()
            .find(|(p, _)| *p == path)
            .map(|(_, b)| b.as_slice())
    }

    /// The fixture fields that name the content, hex-encoded. Each mode adds
    /// its own fields to them.
    fn fixture(&self) -> serde_json::Map<String, serde_json::Value> {
        let files: serde_json::Map<_, _> = self
            .files
            .iter()
            .map(|(path, bytes)| ((*path).to_owned(), alloy::hex::encode(bytes).into()))
            .collect();
        let mut out = serde_json::Map::new();
        out.insert(
            "bundle".to_owned(),
            blake3::hash(&self.manifest).to_hex().to_string().into(),
        );
        out.insert("files".to_owned(), files.into());
        out.insert(
            "blob".to_owned(),
            blake3::hash(&self.blob).to_hex().to_string().into(),
        );
        out.insert(
            "blobBytes".to_owned(),
            alloy::hex::encode(&self.blob).into(),
        );
        out
    }
}

/// What the public run's suite signs pointers with: the deployment, the
/// publisher's key, and the namespaces the key owns, one per client flavour.
struct PublicPointer<'a> {
    chain_id: u64,
    /// The `PublisherRegistry` address.
    registry: &'a str,
    /// Flavour `i` points `namespaces[i]`.
    namespaces: &'a [u64],
    /// The publisher's private key, hex.
    private_key: &'a str,
}

/// The public run's fixture: `content`'s fields, the gateway that serves
/// `http://decdn.test` on `connect`, and `pointer`. Every u64 is a decimal
/// string. `pointer.namespace` is the first of `pointer.namespaces`.
///
/// # Errors
///
/// Fails when `pointer` names no namespace.
fn public_fixture(
    content: &Content,
    connect: &str,
    pointer: &PublicPointer<'_>,
) -> anyhow::Result<serde_json::Map<String, serde_json::Value>> {
    let first = pointer
        .namespaces
        .first()
        .context("the public fixture needs at least one namespace")?;
    let namespaces: Vec<String> = pointer.namespaces.iter().map(u64::to_string).collect();
    let mut fixture = content.fixture();
    fixture.insert("mode".to_owned(), Mode::Public.label().into());
    fixture.insert("gatewayUrl".to_owned(), format!("http://{DOMAIN}").into());
    fixture.insert("connect".to_owned(), connect.into());
    fixture.insert(
        "pointer".to_owned(),
        serde_json::json!({
            "chainId": pointer.chain_id.to_string(),
            "publisherRegistry": pointer.registry,
            "namespace": first.to_string(),
            "namespaces": namespaces,
            "privateKey": pointer.private_key,
        }),
    );
    Ok(fixture)
}

/// The loopback run's fixture: `content`'s fields, the gateway at `base` on
/// `connect` with its `token`, and `grants`, one `(page host, dcap1:
/// capability)` per client flavour. `host` and `capability` are the first
/// grant's.
///
/// # Errors
///
/// Fails when `grants` is empty.
fn loopback_fixture(
    content: &Content,
    base: &str,
    connect: &str,
    token: &str,
    grants: &[(&str, &str)],
) -> anyhow::Result<serde_json::Map<String, serde_json::Value>> {
    let (host, capability) = grants
        .first()
        .context("the loopback fixture needs at least one capability")?;
    let capabilities: Vec<serde_json::Value> = grants
        .iter()
        .map(|(host, capability)| serde_json::json!({ "host": host, "capability": capability }))
        .collect();
    let mut fixture = content.fixture();
    fixture.insert("mode".to_owned(), Mode::Loopback.label().into());
    fixture.insert("gatewayUrl".to_owned(), base.into());
    fixture.insert("connect".to_owned(), connect.into());
    fixture.insert("token".to_owned(), token.into());
    fixture.insert("host".to_owned(), (*host).into());
    fixture.insert("capability".to_owned(), (*capability).into());
    fixture.insert("capabilities".to_owned(), capabilities.into());
    Ok(fixture)
}

#[allow(
    clippy::too_many_lines,
    reason = "one sequential setup: node, publisher, operator, gateway, then the suite"
)]
async fn public<S: Suite>(suite: &S) -> anyhow::Result<()> {
    ensure_decdn_cli_built()?;
    let chain = ChainFixture::launch().await?;
    let content = Content::new();
    let (node, hashes) = NodeFixture::launch_with_blobs(&chain, "US", &content.blobs()).await?;
    let blob_hash = *hashes
        .first()
        .context("the node returned no hash for the blob")?;

    // ---- PUBLISHER: one namespace its key owns per client flavour ----
    let publisher_dir = private_tempdir("publisher")?;
    eth_identity::generate_and_persist(publisher_dir.path(), KEYSTORE_PASSWORD, false)
        .context("generate the publisher keystore")?;
    let publisher = eth_identity::load_signer(
        &eth_identity::keystore_path(publisher_dir.path()),
        KEYSTORE_PASSWORD,
    )?;
    chain.fund_eth(publisher.address(), 1).await?;
    let registry = format!("{}", chain.addrs().publisher_registry);
    let mut namespaces = Vec::with_capacity(MAX_FLAVOURS);
    for _ in 0..MAX_FLAVOURS {
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
        namespaces.push(
            created
                .get("namespace_id")
                .and_then(serde_json::Value::as_u64)
                .with_context(|| format!("namespace_id in {created}"))?,
        );
    }

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
    let http = http_client()?;
    // The gateway's first pull opens its pool, and the node serves it once
    // its chain watcher has seen the pool: the suite starts after that.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        still_running(&mut gateway, "start-up")?;
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

    let fixture = public_fixture(
        &content,
        &listen,
        &PublicPointer {
            chain_id: chain.chain_id(),
            registry: &registry,
            namespaces: &namespaces,
            private_key: &format!("{}", publisher.to_bytes()),
        },
    )?;
    run_file(suite, Mode::Public, &fixture.into()).await?;

    // Only the suite's own `pointer.set` gives a namespace host a bundle, and
    // flavour `i` points `namespaces[i]`.
    for namespace in namespaces.iter().take(suite.flavours()) {
        let page = http
            .get(format!("{base}/a.txt"))
            .header("host", format!("{namespace}.{DOMAIN}"))
            .send()
            .await
            .with_context(|| format!("read the namespace host {namespace}.{DOMAIN}"))?;
        let status = page.status();
        let body = page.bytes().await?;
        anyhow::ensure!(
            status == StatusCode::OK && content.file("a.txt") == Some(body.as_ref()),
            "the namespace host {namespace}.{DOMAIN} does not serve the bundle ({status}): the \
             suite never set the pointer of namespace {namespace} on this gateway"
        );
    }
    stop(gateway).await
}

#[allow(
    clippy::too_many_lines,
    reason = "one sequential setup: node, owner, gateway, capability, then the suite"
)]
async fn loopback<S: Suite>(suite: &S) -> anyhow::Result<()> {
    ensure_decdn_cli_built()?;
    let chain = ChainFixture::launch().await?;
    let content = Content::new();
    let warm = bytes(64 * 1024, 0x0C0F_FEE3);
    let mut blobs = content.blobs();
    blobs.push(&warm);
    let (node, hashes) = NodeFixture::launch_with_blobs(&chain, "US", &blobs).await?;
    let hash_warm = *hashes
        .last()
        .context("the node returned no hash for the warm-up blob")?;

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
    let http = http_client()?;
    let token = wait_for_token(
        &mut gateway,
        &http,
        &gateway_dir.path().join("token"),
        &base,
    )
    .await?;

    // ---- Per client flavour: a delegate the gateway holds, and a capability
    // the owner issues it from the one pool ----
    let mut delegates = Vec::with_capacity(PAGE_HOSTS.len());
    let mut capabilities = Vec::with_capacity(PAGE_HOSTS.len());
    for _ in PAGE_HOSTS {
        let minted: serde_json::Value = http
            .get(format!("{base}/_delegate"))
            .header("x-decdn-token", &token)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .context("mint a delegate with the gateway token")?
            .json()
            .await?;
        let delegate = minted
            .get("address")
            .and_then(serde_json::Value::as_str)
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
            .with_context(|| format!("the dcap1: token in `pool assign` output for {delegate}"))?
            .to_owned();
        delegates.push(delegate);
        capabilities.push(capability);
    }

    let grants: Vec<(&str, &str)> = PAGE_HOSTS
        .into_iter()
        .zip(capabilities.iter().map(String::as_str))
        .collect();
    let fixture = loopback_fixture(&content, &base, &listen, &token, &grants)?;
    run_file(suite, Mode::Loopback, &fixture.into()).await?;

    // Only the suite's own `registerCapability` gives a delegate a scope, and
    // flavour `i` registers `delegates[i]` for `PAGE_HOSTS[i]`.
    let held: serde_json::Value = http
        .get(format!("{base}/_capability"))
        .header("x-decdn-token", &token)
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .context("list the gateway's capabilities")?
        .json()
        .await?;
    for (delegate, host) in delegates.iter().zip(PAGE_HOSTS).take(suite.flavours()) {
        let registered = held.as_array().is_some_and(|held| {
            held.iter().any(|c| {
                c["signer"]
                    .as_str()
                    .is_some_and(|s| s.eq_ignore_ascii_case(delegate))
                    && c["scope"] == host
            })
        });
        anyhow::ensure!(
            registered,
            "the gateway holds no capability for {delegate} on {host}: the suite never \
             registered it on this gateway ({held})"
        );
    }
    stop(gateway).await
}

/// Write `fixture` to a fresh directory and run `suite`'s file for `mode`
/// against it, requiring every test it collects to pass: a skipped or todo
/// test proves nothing about the gateway.
async fn run_file<S: Suite>(
    suite: &S,
    mode: Mode,
    fixture: &serde_json::Value,
) -> anyhow::Result<()> {
    let dir = tempfile::tempdir().context("fixture tempdir")?;
    let path = dir.path().join("contract.json");
    std::fs::write(&path, serde_json::to_vec_pretty(fixture)?).context("write the fixture")?;
    let Outcome { total, passed } = suite.run(mode, &path, dir.path()).await?;
    anyhow::ensure!(
        total > 0 && passed == total,
        "{} {}: {passed} of {total} contract tests passed; a skipped or todo test proves nothing",
        suite.name(),
        mode.label()
    );
    Ok(())
}

/// An HTTP client whose every request gives up after [`REQUEST_TIMEOUT`].
fn http_client() -> anyhow::Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .build()
        .context("build the HTTP client")
}

/// Fail when `gateway` has exited, naming `when` and its exit status.
fn still_running(gateway: &mut tokio::process::Child, when: &str) -> anyhow::Result<()> {
    if let Some(status) = gateway.try_wait().context("poll the gateway")? {
        anyhow::bail!("decdn gateway exited during {when}: {status}");
    }
    Ok(())
}

/// Stop `gateway` after the suite, failing if it had already exited: a
/// gateway that crashed under the suite fails the run.
async fn stop(mut gateway: tokio::process::Child) -> anyhow::Result<()> {
    still_running(&mut gateway, "the suite")?;
    gateway.kill().await.context("stop the gateway")
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

/// The gateway token, once the gateway has written all of it and answers on
/// `base`.
async fn wait_for_token(
    gateway: &mut tokio::process::Child,
    http: &reqwest::Client,
    path: &Path,
    base: &str,
) -> anyhow::Result<String> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        still_running(gateway, "start-up")?;
        let token = std::fs::read_to_string(path).unwrap_or_default();
        let token = token.trim();
        if token.len() == 64
            && token.bytes().all(|b| b.is_ascii_hexdigit())
            && http.get(format!("{base}/_capability")).send().await.is_ok()
        {
            return Ok(token.to_owned());
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
    let raw = parsed
        .get("poolId")
        .and_then(serde_json::Value::as_str)
        .with_context(|| format!("no poolId in {stdout:?}"))?;
    B256::from_str(raw).context("poolId is not a 32-byte hash")
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
