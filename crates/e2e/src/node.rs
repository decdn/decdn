//! Node fixture: a `decdn-node` daemon (spawned as a subprocess) wired to a
//! [`crate::chain::ChainFixture`] deployment.
//!
//! The daemon is run as a real subprocess rather than via `runtime::run`
//! in-process because the runtime installs **process-global** SIGHUP/SIGTERM
//! signal streams and `commands::run` sets a **process-global** tracing
//! subscriber — both make N-in-one-process daemons unsafe. A subprocess per
//! node sidesteps that, gives true signal-based graceful shutdown, and is the
//! most faithful end-to-end shape (it also matches how `anvil`/`forge` are
//! already shelled out). The cost is that the `decdn-node` binary must be built
//! first (`cargo build -p decdn-node`); `decdn_node_bin` locates it relative
//! to the test executable and errors clearly if absent.

use std::path::PathBuf;
use std::process::Child;
use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::Address;
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use decdn_cache::{CacheEngine, FilesystemOrigin, Hash, Origin};
use decdn_common::admin::AdminRpcClient;
use decdn_common::identity;
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};

use crate::chain::ChainFixture;

/// Fixed keystore password for the daemon's eth signer (test-only). Public so a
/// journey that drives the `decdn` CLI against this node's keystore can pass it
/// via `DECDN_KEYSTORE_PASSWORD` (#1032).
pub const KEYSTORE_PASSWORD: &str = "decdn-e2e-test-password";

/// Kills the spawned `decdn-node` on drop so a panicking assertion never leaks
/// the daemon process. The `Child` is behind a `Mutex` so [`NodeFixture::wait_healthy`]
/// can `try_wait` it through a shared `&self` reference.
#[derive(Debug)]
struct NodeGuard(std::sync::Mutex<Child>);

impl Drop for NodeGuard {
    fn drop(&mut self) {
        // Poison-tolerant: run cleanup even if a panic poisoned the lock.
        let mut child = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _ = child.kill();
        let _ = child.wait();
    }
}

/// A live `decdn-node` daemon onboarded against a chain deployment, serving one
/// pre-seeded blob via a filesystem pull-through origin.
#[derive(Debug)]
pub struct NodeFixture {
    child: NodeGuard,
    // TempDirs kept alive for the daemon's lifetime.
    _data_dir: tempfile::TempDir,
    _origin_dir: tempfile::TempDir,
    /// The operator's Ethereum signer (also the on-chain bond/settlement key).
    pub operator: PrivateKeySigner,
    /// The operator's Ethereum address.
    pub operator_addr: Address,
    /// The node's iroh identity (its `NodeId`), for clients to dial.
    pub node_id: iroh::PublicKey,
    /// QUIC bind port (loopback) the client dials.
    pub bind_port: u16,
    /// Loopback admin RPC base URL.
    pub admin_url: String,
    /// Path to the rendered `node.toml`, so a journey can point the `decdn` CLI
    /// at the same `[blockchain]` coordinates + keystore the daemon uses (#1032).
    pub config_path: PathBuf,
}

impl NodeFixture {
    /// Provision a data dir + keystore + iroh key, onboard the operator
    /// on-chain, write the daemon config (with a filesystem origin holding
    /// `serve_blob`), spawn `decdn-node run`, and wait until its admin RPC is
    /// healthy. Returns the fixture and the BLAKE3 [`struct@Hash`] of the served blob.
    pub async fn launch(
        chain: &ChainFixture,
        region: &str,
        serve_blob: &[u8],
    ) -> anyhow::Result<(Self, Hash)> {
        let data_dir = tempfile::tempdir().context("create node data dir")?;
        // `identity::ensure_data_dir` (and the keystore/identity writers) require
        // an `0o700` data dir; a umask of 022 leaves the tempdir at 0o755, so
        // tighten it explicitly first (mirrors the anvil settlement e2e).
        #[cfg(unix)]
        std::fs::set_permissions(
            data_dir.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .context("chmod data dir 0o700")?;
        identity::ensure_data_dir(data_dir.path()).context("tighten data dir perms")?;

        // Operator eth keystore + signer.
        decdn_incentive::eth_identity::generate_and_persist(
            data_dir.path(),
            KEYSTORE_PASSWORD,
            false,
        )
        .context("generate operator keystore")?;
        let keystore = decdn_incentive::eth_identity::keystore_path(data_dir.path());
        let operator = decdn_incentive::eth_identity::load_signer(&keystore, KEYSTORE_PASSWORD)
            .context("load operator signer")?;
        let operator_addr = operator.address();

        // iroh node key (persisted so the daemon serves under it).
        let node_secret = identity::load_or_generate(data_dir.path()).context("node iroh key")?;
        let node_id = node_secret.public();

        // Seed the blob into a filesystem origin: `{root}/{hex[..2]}/{hex}`.
        let origin_dir = tempfile::tempdir().context("create origin dir")?;
        let hash = Hash::new(serve_blob);
        write_fs_origin_blob(origin_dir.path(), &hash, serve_blob)?;

        let bind_port = crate::free_port()?;
        let admin_port = crate::free_port()?;
        let metrics_port = crate::free_port()?;
        let multiaddr = format!("/ip4/127.0.0.1/udp/{bind_port}/quic-v1");

        chain
            .onboard_operator(&operator, &node_secret, region, &multiaddr)
            .await
            .context("onboard operator on-chain")?;

        let cache_dir = data_dir.path().join("cache");
        let config = render_config(&RenderConfig {
            data_dir: data_dir.path().to_path_buf(),
            region,
            bind_port,
            admin_port,
            metrics_port,
            rpc_url: &chain.rpc_url,
            keystore: &keystore,
            cache_dir: &cache_dir,
            origin_dir: origin_dir.path(),
            chain,
        });
        let config_path = data_dir.path().join("node.toml");
        std::fs::write(&config_path, config).context("write node config")?;

        // Pre-warm the node's cache store from the filesystem origin. The
        // `cdn/client/v1` serve path gates on local-store presence
        // (`cache.has`); origin pull-through is NOT consulted on the direct
        // client path (node-to-node pull-through is off by default and a plain
        // `stream_fetch` client sends no ownership binding to authorize spend).
        // So an operator serves content it already holds — warm it the same way
        // production does, via the origin engine, then let the daemon reopen the
        // populated store. Mirrors the node integration `cache_with_blob` helper.
        std::fs::create_dir_all(&cache_dir).context("create cache dir")?;
        {
            let origin: Arc<dyn Origin> = Arc::new(
                FilesystemOrigin::new(origin_dir.path())
                    .await
                    .context("open fs origin for warm")?,
            );
            let warm = CacheEngine::open(&cache_dir, vec![origin], 1024)
                .await
                .context("open warm cache")?;
            warm.get(hash)
                .await
                .context("warm node cache from fs origin")?;
            // Explicitly flush the iroh-blobs store to disk so the daemon's
            // reopen of `cache_dir` sees the blob (drop alone does not sync).
            warm.shutdown().await.context("flush warm cache")?;
        }

        let child = std::process::Command::new(decdn_node_bin()?)
            .arg("--config")
            .arg(&config_path)
            .arg("run")
            .env("DECDN_KEYSTORE_PASSWORD", KEYSTORE_PASSWORD)
            // Quiet by default; flip to `info`/`debug` when debugging a failure.
            .env(
                "RUST_LOG",
                std::env::var("DECDN_NODE_LOG").unwrap_or_else(|_| "warn".into()),
            )
            .spawn()
            .context("spawn decdn-node")?;

        let fixture = Self {
            child: NodeGuard(std::sync::Mutex::new(child)),
            _data_dir: data_dir,
            _origin_dir: origin_dir,
            operator,
            operator_addr,
            node_id,
            bind_port,
            admin_url: format!("http://127.0.0.1:{admin_port}"),
            config_path,
        };
        fixture
            .wait_healthy(Duration::from_secs(30))
            .await
            .context("node never became healthy")?;
        Ok((fixture, hash))
    }

    /// Build a loopback admin JSON-RPC client for this node.
    pub fn admin_client(&self) -> anyhow::Result<HttpClient> {
        HttpClientBuilder::default()
            .request_timeout(Duration::from_secs(5))
            .build(&self.admin_url)
            .with_context(|| format!("build admin client for {}", self.admin_url))
    }

    /// Poll `admin_v1_health` until it succeeds or `timeout` elapses. Fails fast
    /// if the daemon process exits before becoming healthy (e.g. bad config /
    /// port clash) rather than waiting out the full timeout.
    pub async fn wait_healthy(&self, timeout: Duration) -> anyhow::Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        // Remember the last probe error so a persistent-but-alive failure mode
        // surfaces its cause instead of a bare "not healthy" timeout. Always
        // written before the deadline read below, so it starts uninitialized.
        let mut last_err: Option<String>;
        loop {
            match self.admin_client() {
                Ok(client) => match client.health().await {
                    Ok(_) => return Ok(()),
                    Err(e) => last_err = Some(e.to_string()),
                },
                Err(e) => last_err = Some(e.to_string()),
            }
            // Detect a daemon that died at startup. Scope the lock so the guard
            // is dropped before the `sleep().await` (never hold a std `Mutex`
            // across an await point).
            {
                let mut child = self
                    .child
                    .0
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Ok(Some(status)) = child.try_wait() {
                    anyhow::bail!(
                        "decdn-node exited prematurely before becoming healthy: {status}"
                    );
                }
            }
            if tokio::time::Instant::now() >= deadline {
                let cause = last_err
                    .map(|e| format!(" (last probe error: {e})"))
                    .unwrap_or_default();
                anyhow::bail!(
                    "admin RPC at {} not healthy within {timeout:?}{cause}",
                    self.admin_url
                );
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
}

/// Inputs for [`render_config`].
struct RenderConfig<'a> {
    data_dir: PathBuf,
    region: &'a str,
    bind_port: u16,
    admin_port: u16,
    metrics_port: u16,
    rpc_url: &'a str,
    keystore: &'a std::path::Path,
    cache_dir: &'a std::path::Path,
    origin_dir: &'a std::path::Path,
    chain: &'a ChainFixture,
}

/// Render the daemon TOML config. Emits only the keys the fixture sets; the
/// daemon fills the rest from defaults. `event_poll_interval_ms` is dropped to
/// 500ms so chain watchers react quickly against the local anvil.
fn render_config(c: &RenderConfig<'_>) -> String {
    let a = &c.chain.addrs;
    // Path fields use single-quoted TOML *literal* strings so backslashes in a
    // Windows path (or any stray escape) round-trip verbatim. The other string
    // values are controlled (alpha-2 region, `http://127.0.0.1:port` RPC, hex
    // addresses) and stay double-quoted.
    format!(
        r#"[identity]
data_dir = '{data_dir}'
region = "{region}"

[network]
bind_port = {bind_port}

[blockchain]
rpc_url = "{rpc_url}"
eth_keystore = '{keystore}'
chain_id = {chain_id}
payment_channel_address = "{payment_channel}"
capacity_bond_address = "{capacity_bond}"
slash_judge_address = "{slash_judge}"
slash_appeal_address = "{slash_appeal}"
publisher_registry_address = "{publisher_registry}"
origin_assignment_address = "{origin_assignment}"
event_poll_interval_ms = 500
redeem_threshold_micro_usdc = 10

[cache]
cache_dir = '{cache_dir}'
cache_size_mb = 4096

[cache.origin]
kind = "fs"
path = '{origin_dir}'

[payment]
rate_per_mb = 10

[observability]
log_level = "warn"
log_format = "pretty"
admin_port = {admin_port}
metrics_port = {metrics_port}
metrics_bind = "127.0.0.1"
"#,
        data_dir = c.data_dir.display(),
        region = c.region,
        bind_port = c.bind_port,
        rpc_url = c.rpc_url,
        keystore = c.keystore.display(),
        chain_id = c.chain.chain_id,
        payment_channel = a.payment_channel,
        capacity_bond = a.capacity_bond,
        slash_judge = a.slash_judge,
        slash_appeal = a.slash_appeal,
        publisher_registry = a.publisher_registry,
        origin_assignment = a.origin_assignment,
        cache_dir = c.cache_dir.display(),
        origin_dir = c.origin_dir.display(),
        admin_port = c.admin_port,
        metrics_port = c.metrics_port,
    )
}

/// Write `blob` into a filesystem-origin shard layout (`{root}/{hex[..2]}/{hex}`).
fn write_fs_origin_blob(root: &std::path::Path, hash: &Hash, blob: &[u8]) -> anyhow::Result<()> {
    let hex = hash.to_hex();
    let shard = hex.as_str().get(..2).context("blob hex too short")?;
    let dir = root.join(shard);
    std::fs::create_dir_all(&dir).context("create origin shard dir")?;
    std::fs::write(dir.join(hex.as_str()), blob).context("write origin blob")?;
    Ok(())
}

/// Locate the built `decdn-node` binary relative to the current test executable
/// (`target/<profile>/decdn-node`), falling back to `DECDN_NODE_BIN`.
fn decdn_node_bin() -> anyhow::Result<PathBuf> {
    if let Some(p) = std::env::var_os("DECDN_NODE_BIN") {
        return Ok(PathBuf::from(p));
    }
    let exe = std::env::current_exe().context("current_exe")?;
    // .../target/<profile>/deps/<test-bin>  → .../target/<profile>/decdn-node
    let profile_dir = exe
        .parent()
        .and_then(|deps| deps.parent())
        .context("resolve target profile dir")?;
    let bin = profile_dir.join(if cfg!(windows) {
        "decdn-node.exe"
    } else {
        "decdn-node"
    });
    anyhow::ensure!(
        bin.exists(),
        "decdn-node binary not found at {}; run `cargo build -p decdn-node` first \
         (or set DECDN_NODE_BIN)",
        bin.display()
    );
    Ok(bin)
}
