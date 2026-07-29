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

use std::fmt::Write as _;
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

use crate::chain::{ChainFixture, ContractAddrs};

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
///
/// The descriptive handles (operator keys/address, node id, ports, admin URL,
/// config path) are facts about the already-running daemon, fixed at
/// [`Self::launch`]. They are exposed as accessors rather than `pub` fields so a
/// caller can't reassign one and silently desync the fixture from the live
/// process it describes.
#[derive(Debug)]
pub struct NodeFixture {
    child: NodeGuard,
    // TempDirs kept alive for the daemon's lifetime. The data dir is also read
    // back by `data_dir()` as an isolated `HOME` for CLI subprocesses.
    data_dir: tempfile::TempDir,
    // The node's OPAQUE backend (`[cache.origin] kind = "fs"`). Kept alive for
    // the daemon's lifetime and writable after launch via
    // [`NodeFixture::seed_origin_blob`], so a journey can model content the
    // operator holds in its backend but has never cached.
    origin_dir: tempfile::TempDir,
    operator: PrivateKeySigner,
    operator_addr: Address,
    node_id: iroh::PublicKey,
    bind_port: u16,
    // Prometheus scrape port (loopback), retained so a journey can read a
    // per-reason reject counter directly (`scrape_metric`) rather than infer it
    // from the collapsed wire `NotFound` (#1371).
    metrics_port: u16,
    admin_url: String,
    // Rendered `node.toml`: respawns the daemon on `restart` against the same
    // state; `config_path()` lets a journey point the `decdn` CLI at the same
    // `[blockchain]` coordinates + keystore the daemon uses (#1032).
    config_path: PathBuf,
}

impl NodeFixture {
    /// Provision + launch a node serving a single blob. Returns the fixture and
    /// the served blob's BLAKE3 [`struct@Hash`].
    pub async fn launch(
        chain: &ChainFixture,
        region: &str,
        serve_blob: &[u8],
    ) -> anyhow::Result<(Self, Hash)> {
        let (node, mut hashes) = Self::launch_with_blobs(chain, region, &[serve_blob]).await?;
        let hash = hashes
            .pop()
            .ok_or_else(|| anyhow::anyhow!("launch_with_blobs returned no hash"))?;
        Ok((node, hash))
    }

    /// The operator's Ethereum signer (also the on-chain bond/settlement key).
    #[must_use]
    pub const fn operator(&self) -> &PrivateKeySigner {
        &self.operator
    }

    /// The operator's Ethereum address.
    #[must_use]
    pub const fn operator_addr(&self) -> Address {
        self.operator_addr
    }

    /// The node's iroh identity (its `NodeId`), for clients to dial.
    #[must_use]
    pub const fn node_id(&self) -> iroh::PublicKey {
        self.node_id
    }

    /// QUIC bind port (loopback) the client dials.
    #[must_use]
    pub const fn bind_port(&self) -> u16 {
        self.bind_port
    }

    /// Loopback admin RPC base URL.
    #[must_use]
    pub fn admin_url(&self) -> &str {
        &self.admin_url
    }

    /// Path to the rendered `node.toml`: lets a journey point the `decdn` CLI at
    /// the same `[blockchain]` coordinates + keystore the daemon uses (#1032).
    #[must_use]
    pub fn config_path(&self) -> &std::path::Path {
        &self.config_path
    }

    /// Rewrite `payment.rate_per_mb` in the daemon's config and hot-reload it via
    /// `admin_v1_reload`, returning the rate the daemon reports post-reload.
    ///
    /// `payment.rate_per_mb` is in the reloadable set (`runtime::reload`), and the
    /// probe and client handlers both read it through the atomic the reload swaps
    /// — so this changes the quoted rate of a *running* daemon between two
    /// requests, with no restart and no reconnect. That is precisely the
    /// rate bait-and-switch a `SlashJudge` rate challenge exists to punish
    /// (#1042), induced through the operator's real config surface rather than
    /// simulated.
    ///
    /// The returned value is the daemon's post-reload **configured** rate — a bare
    /// load of the atomic the reload swapped (`runtime::reload`'s `ReloadSnapshot`),
    /// read back over the admin RPC. It proves the hot-swap landed; it is *not* the
    /// rate the node will sign. Clamping into the on-chain `[floor, ceiling]` band
    /// happens later and per-response, off that same atomic
    /// (`handlers::client::wire::clamped_rate`, and its probe twin). So a caller
    /// that needs `stream > probe` must assert on the `rate_per_mb` inside the
    /// captured `StreamResponse`/`ProbeResponse` — the signed bytes that become
    /// evidence — never on this return value, which would happily report an
    /// out-of-band rate the daemon then clamps away.
    pub async fn set_rate_per_mb(&self, rate: u64) -> anyhow::Result<u64> {
        let config = std::fs::read_to_string(&self.config_path).context("read node config")?;
        let rewritten = rewrite_rate_per_mb(&config, rate)?;
        std::fs::write(&self.config_path, rewritten).context("write node config")?;

        let resp = self
            .admin_client()?
            .reload()
            .await
            .context("admin_v1_reload")?;
        Ok(resp.rate_per_mb)
    }

    /// The daemon's data dir (`0o700` on Unix). Doubles as the `HOME` a journey hands
    /// [`crate::cli::decdn_command`] when it drives the `decdn` CLI against
    /// this node's config + keystore (#1332).
    #[must_use]
    pub fn data_dir(&self) -> &std::path::Path {
        self.data_dir.path()
    }

    /// Provision a data dir + keystore + iroh key, onboard the operator
    /// on-chain, write the daemon config (with a filesystem origin holding
    /// `serve_blobs`), spawn `decdn-node run`, and wait until its admin RPC is
    /// healthy. Returns the fixture and the BLAKE3 [`struct@Hash`] of each blob,
    /// in input order. Multiple blobs let a journey hold a sentinel alongside the
    /// blob under test (e.g. an ordering barrier for event processing).
    pub async fn launch_with_blobs(
        chain: &ChainFixture,
        region: &str,
        serve_blobs: &[&[u8]],
    ) -> anyhow::Result<(Self, Vec<Hash>)> {
        Self::launch_configured(chain, region, serve_blobs, false, false, &[]).await
    }

    /// Launch an empty bonded cache node whose misses use paid node-to-node
    /// pull-through, gated by the chain-backed authorized-origin directory.
    pub async fn launch_pull_through_cache(
        chain: &ChainFixture,
        region: &str,
        discovery_peers: &[&NodeFixture],
    ) -> anyhow::Result<Self> {
        let (node, hashes) =
            Self::launch_configured(chain, region, &[], true, true, discovery_peers).await?;
        anyhow::ensure!(
            hashes.is_empty(),
            "empty cache launch returned seeded hashes"
        );
        Ok(node)
    }

    /// Launch a bonded node that **owns an opaque origin backend** and gates its
    /// reactive cache-miss fill on the chain-backed authorized-origin directory
    /// (`cache.pull_through_require_authorized_origin`, #821 / ADR 037).
    ///
    /// `cached_blobs` are pre-warmed into the node's local store, so they are
    /// served from cache regardless of what the chain says. Blobs written *after*
    /// launch with [`Self::seed_origin_blob`] exist ONLY in the backend, so
    /// serving them requires the gate to be open — which is the distinction
    /// G-NODE-08 asserts.
    ///
    /// Note what the gate actually checks: `pull_origin_gate_blocks` asks whether
    /// the hash's **namespace** has any currently-authorized active origin, not
    /// whether *this* operator is one of them. A namespace ratified to a
    /// different operator would also open this node's gate. Whether that is the
    /// intended scope is tracked in #1368; this fixture deliberately does not
    /// depend on either reading.
    ///
    /// No discovery peers, so the node has no upstream and a served backend-only
    /// blob can only have come from its own `[cache.origin]`. `render_config`
    /// exposes the two knobs independently (#1376), but this helper still arms
    /// both: `pull_through_require_authorized_origin` is only *wired* by the
    /// daemon inside `if node_to_node_pull_through_enabled` (`runtime::mod`), so
    /// the gate needs node→node pull-through on to take effect. The no-upstream
    /// guarantee therefore rests on the empty peer set, not on the flag being off.
    pub async fn launch_authorized_origin(
        chain: &ChainFixture,
        region: &str,
        cached_blobs: &[&[u8]],
    ) -> anyhow::Result<(Self, Vec<Hash>)> {
        Self::launch_configured(chain, region, cached_blobs, true, true, &[]).await
    }

    /// Write `blob` into the node's opaque origin backend **without** touching its
    /// cache store, and return its BLAKE3 [`struct@Hash`]. The `fs` origin adapter
    /// reads per request, so a post-launch write is visible to the running daemon.
    ///
    /// Models "the operator's backend holds `H`, the operator has never served
    /// it": the only way this blob reaches a client is the reactive local-origin
    /// pull-through (`populate_local`), which is what the authorized-origin gate
    /// stands in front of.
    pub fn seed_origin_blob(&self, blob: &[u8]) -> anyhow::Result<Hash> {
        let hash = Hash::new(blob);
        write_fs_origin_blob(self.origin_dir.path(), &hash, blob)?;
        Ok(hash)
    }

    /// Like [`Self::seed_origin_blob`], but also writes the sibling `{H}.obao4`
    /// pre-order outboard so a ranged origin fetch reaches the **range tier**.
    ///
    /// `FilesystemOrigin::fetch_range` treats a missing outboard as `Unsupported`
    /// by design, so a blob seeded by `seed_origin_blob` alone declines the range
    /// path and falls through to a whole-blob fill — the origin range tier and
    /// `bao-range`'s chunk-group handling are then never exercised (#1372). Seed
    /// with this variant when a journey asserts on the range path itself.
    pub fn seed_origin_blob_with_outboard(&self, blob: &[u8]) -> anyhow::Result<Hash> {
        let hash = Hash::new(blob);
        write_fs_origin_blob(self.origin_dir.path(), &hash, blob)?;
        write_fs_origin_outboard(self.origin_dir.path(), &hash, blob)?;
        Ok(hash)
    }

    /// Filesystem path of the node's opaque origin backend. Journeys assert this
    /// string never reaches the wire — origin backends are per-node config and
    /// MUST stay invisible to clients.
    ///
    /// This is the *configured* path. `FilesystemOrigin::new` canonicalizes at
    /// construction, so the daemon actually holds the resolved form (on macOS,
    /// `/private/var/…` for a `/var/…` tempdir). A leak would carry the canonical
    /// bytes, so an opacity scan should search both forms.
    #[must_use]
    pub fn origin_root(&self) -> &std::path::Path {
        self.origin_dir.path()
    }

    /// Poll `admin_v1_channels` until the daemon's settlement watcher has
    /// registered `channel_id` (or `timeout` elapses).
    ///
    /// The pre-observation window and a genuine refusal are the SAME wire
    /// `NotFound` (`ServeRejectReason::wire_error` collapses both), so a journey
    /// that must assert on a single refusal cannot use
    /// [`crate::client::ClientFixture::fetch`]'s retry loop to ride the window
    /// out. Waiting on the node's own view of the channel narrows it.
    ///
    /// **On its own it does not close that window.** `admin_v1_channels` reads
    /// the *persisted* channel store, whereas `serve_stream` / `pull_authorized`
    /// gate on `ClientHandler`'s in-memory map — and `register_open_channel`
    /// awaits the store fsync *before* inserting into that map. So this can
    /// return while the serve path still answers `UnknownChannel`.
    ///
    /// Prefer [`crate::client::ClientFixture::open_session`], which pairs this
    /// with a retried warm-up fetch; a served blob is what actually proves the
    /// live map is populated. Call this directly only to assert on the node's
    /// bookkeeping itself.
    pub async fn wait_for_channel(
        &self,
        channel_id: alloy::primitives::B256,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        let admin = self.admin_client()?;
        let wanted = channel_id.to_string();
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let known = admin
                .channels()
                .await
                .context("admin channels")?
                .channels
                .into_iter()
                .any(|c| c.channel_id.eq_ignore_ascii_case(&wanted));
            if known {
                return Ok(());
            }
            anyhow::ensure!(
                tokio::time::Instant::now() < deadline,
                "node never observed channel {wanted} within {timeout:?}"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    async fn launch_configured(
        chain: &ChainFixture,
        region: &str,
        serve_blobs: &[&[u8]],
        node_to_node_pull_through: bool,
        require_authorized_origin: bool,
        discovery_peers: &[&NodeFixture],
    ) -> anyhow::Result<(Self, Vec<Hash>)> {
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

        // Seed each blob into a filesystem origin: `{root}/{hex[..2]}/{hex}`.
        let origin_dir = tempfile::tempdir().context("create origin dir")?;
        let hashes: Vec<Hash> = serve_blobs
            .iter()
            .map(|blob| {
                let hash = Hash::new(blob);
                write_fs_origin_blob(origin_dir.path(), &hash, blob)?;
                Ok(hash)
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        // One simultaneous grab so the three ports are guaranteed distinct
        // (three sequential grabs can repeat a number).
        let [bind_port, admin_port, metrics_port] = crate::free_ports::<3>()?;
        let multiaddr = format!("/ip4/127.0.0.1/udp/{bind_port}/quic-v1");

        chain
            .onboard_operator(&operator, &node_secret, region, &multiaddr)
            .await
            .context("onboard operator on-chain")?;

        let cache_dir = data_dir.path().join("cache");
        // Bind to a local so the `&str` field borrows a value that clearly
        // outlives the `render_config` call (not a same-statement temporary).
        let rpc_url = chain.rpc_url();
        let discovery_peers: Vec<_> = discovery_peers
            .iter()
            .map(|peer| (peer.node_id(), peer.bind_port()))
            .collect();
        let config = render_config(&RenderConfig {
            data_dir: data_dir.path().to_path_buf(),
            region,
            bind_port,
            admin_port,
            metrics_port,
            rpc_url: &rpc_url,
            keystore: &keystore,
            cache_dir: &cache_dir,
            origin_dir: origin_dir.path(),
            chain_id: chain.chain_id(),
            addrs: chain.addrs(),
            node_to_node_pull_through,
            require_authorized_origin,
            discovery_peers: &discovery_peers,
        });
        let config_path = data_dir.path().join("node.toml");
        std::fs::write(&config_path, config).context("write node config")?;

        // Pre-warm the node's cache store from the filesystem origin. The
        // default fixture models content the operator already holds: warm it
        // through the production origin engine, then let the daemon reopen the
        // populated store. The empty pull-through variant has no hashes to warm
        // and instead exercises the bound client's reactive acquisition path.
        // Mirrors the node integration `cache_with_blob` helper.
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
            for hash in &hashes {
                warm.get(*hash)
                    .await
                    .context("warm node cache from fs origin")?;
            }
            // Explicitly flush the iroh-blobs store to disk so the daemon's
            // reopen of `cache_dir` sees the blob (drop alone does not sync).
            warm.shutdown().await.context("flush warm cache")?;
        }

        let child = spawn_daemon(&config_path, data_dir.path())?;

        let fixture = Self {
            child: NodeGuard(std::sync::Mutex::new(child)),
            data_dir,
            origin_dir,
            operator,
            operator_addr,
            node_id,
            bind_port,
            metrics_port,
            admin_url: format!("http://127.0.0.1:{admin_port}"),
            config_path,
        };
        fixture
            .wait_healthy(Duration::from_secs(30))
            .await
            .context("node never became healthy")?;
        Ok((fixture, hashes))
    }

    /// Restart the daemon in place: kill the current `decdn-node` subprocess and
    /// respawn it against the same config + data dir (no re-onboarding — the
    /// operator is already on-chain), then wait until healthy. Proves persisted
    /// state (e.g. durable blacklist eviction via `evicted.log`) survives a
    /// restart, and exercises the across-restart slash re-scan (#1032): the new
    /// process rebuilds its in-memory slash store by re-enumerating on chain and
    /// seeding the watcher tail. Uses `&self`: the child handle lives behind a
    /// `Mutex`, so the swap needs no exclusive borrow.
    pub async fn restart(&self) -> anyhow::Result<()> {
        // Kill the old process and swap in the new one, holding the guard lock
        // only briefly (never across an await). `wait()` reaps the old process
        // so it has released its ports before the replacement binds them.
        {
            let mut child = self
                .child
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let _ = child.kill();
            let _ = child.wait();
            *child = spawn_daemon(&self.config_path, self.data_dir.path())?;
        }
        self.wait_healthy(Duration::from_secs(30))
            .await
            .context("node never became healthy after restart")
    }

    /// Scrape the daemon's Prometheus endpoint and return the current value of a
    /// single unlabelled counter/gauge by name, or `0` if it is absent (a
    /// registered-but-never-incremented counter is reported as `0`).
    ///
    /// Because a **missing** metric reads as `0`, assert on this only as a
    /// *delta* (`after == before + 1`), never as an absolute (`== 0`): a typo'd
    /// or unregistered name returns `0` for both reads, so a delta assertion
    /// still fails loudly (`0 == 0 + 1`) whereas `assert_eq!(…, 0)` would pass
    /// vacuously. All reject counters are eagerly registered, so a correct name
    /// never actually hits the absent-reads-as-zero path.
    ///
    /// This reads a **per-reason** signal directly, which the wire protocol
    /// deliberately hides: `ServeRejectReason::wire_error` collapses seven
    /// distinct reject reasons onto one `StreamError::NotFound`, so an
    /// end-to-end refusal is otherwise indistinguishable from a plain cache miss
    /// (#1371). The counters bump *before* the network write, so a 0→1 step
    /// across a single fetch pins the refusal to its exact cause.
    ///
    /// Assert on a **delta** (`after == before + 1`), never an absolute value.
    /// Because a missing metric reads as `0`, a `0→1` delta catches a misspelled
    /// name (`0 == 0 + 1` fails), but an absence check `assert_eq!(scrape(X), 0)`
    /// would pass *vacuously* for a typo'd `X`. An absolute-value assertion is
    /// also unnecessary here: the reject counters are exposed-at-zero on a fresh
    /// registry, so a delta of 0 already proves non-increment.
    ///
    /// Values are summed across every matching sample; the integer part of each
    /// value is parsed (the Prometheus text format renders a `_total` counter
    /// without a fraction), so a metric that later gains labels still totals up.
    pub async fn scrape_metric(&self, name: &str) -> anyhow::Result<u64> {
        let url = format!("http://127.0.0.1:{}/metrics", self.metrics_port);
        let body = reqwest::get(&url)
            .await
            .with_context(|| format!("scrape {url}"))?
            .error_for_status()
            .context("metrics endpoint status")?
            .text()
            .await
            .context("read metrics body")?;
        let mut total: u64 = 0;
        for line in body.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            // `metric_name value` or `metric_name{labels...} value`. Match the
            // series name up to the first `{` or whitespace.
            let series = line.split_whitespace().next().unwrap_or_default();
            let series_name = series.split('{').next().unwrap_or_default();
            if series_name != name {
                continue;
            }
            let value = line
                .rsplit_once(char::is_whitespace)
                .map(|(_, v)| v)
                .unwrap_or_default();
            // Counters render as bare integers; tolerate a `.0`-style fraction
            // by taking the integer part rather than pulling in float parsing.
            let integer_part = value.split('.').next().unwrap_or_default();
            total = total.saturating_add(
                integer_part
                    .parse::<u64>()
                    .with_context(|| format!("parse metric value {value:?} for {name}"))?,
            );
        }
        // A missing counter reads as 0 (registered-but-never-incremented), which
        // is exactly what a 0→1 assertion across the first refusal wants.
        Ok(total)
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

/// Inputs for [`render_config`]. Holds only the chain coordinates the template
/// needs (`chain_id` + contract `addrs`) rather than the whole
/// [`ChainFixture`], so the renderer is unit-testable without a live chain.
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
    chain_id: u64,
    addrs: ContractAddrs,
    node_to_node_pull_through: bool,
    require_authorized_origin: bool,
    discovery_peers: &'a [(iroh::PublicKey, u16)],
}

/// Render the daemon TOML config. Emits only the keys the fixture sets; the
/// daemon fills the rest from defaults. `event_poll_interval_ms` is dropped to
/// 500ms so chain watchers react quickly against the local anvil.
fn render_config(c: &RenderConfig<'_>) -> String {
    let a = &c.addrs;
    // Path fields use single-quoted TOML *literal* strings so backslashes in a
    // Windows path (or any stray escape) round-trip verbatim. The other string
    // values are controlled (alpha-2 region, `http://127.0.0.1:port` RPC, hex
    // addresses) and stay double-quoted.
    let mut rendered = format!(
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
content_blacklist_address = "{content_blacklist}"
# Small so a scope transition with no on-chain event (region/ripening, appeal
# reversal) is re-scoped within the test budget rather than the 10-min default.
content_blacklist_poll_interval_sec = 2
publisher_registry_address = "{publisher_registry}"
origin_assignment_address = "{origin_assignment}"
event_poll_interval_ms = 500
redeem_threshold_micro_usdc = 10

[cache]
cache_dir = '{cache_dir}'
cache_size_mb = 4096
node_to_node_pull_through_enabled = {node_to_node_pull_through}
pull_through_require_authorized_origin = {require_authorized_origin}

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
        chain_id = c.chain_id,
        payment_channel = a.payment_channel,
        capacity_bond = a.capacity_bond,
        slash_judge = a.slash_judge,
        slash_appeal = a.slash_appeal,
        content_blacklist = a.content_blacklist,
        publisher_registry = a.publisher_registry,
        origin_assignment = a.origin_assignment,
        cache_dir = c.cache_dir.display(),
        node_to_node_pull_through = c.node_to_node_pull_through,
        require_authorized_origin = c.require_authorized_origin,
        origin_dir = c.origin_dir.display(),
        admin_port = c.admin_port,
        metrics_port = c.metrics_port,
    );
    for (node_id, port) in c.discovery_peers {
        let _ = write!(
            &mut rendered,
            "\n[network.discovery.peers.{node_id}]\naddrs = [\"127.0.0.1:{port}\"]\n"
        );
    }
    rendered
}

/// Rewrite `payment.rate_per_mb` in a node TOML config, preserving every other
/// key. Split out of [`NodeFixture::set_rate_per_mb`] so its parse → mutate →
/// `toml::to_string` round-trip is testable without a live daemon (#1378): the
/// serializer's `ValueAfterTable` hazard (a scalar written after a sub-table in
/// `[cache]`) is handled today because each table gets its own buffer, but a
/// `toml` bump could silently break it — this is the plain-`cargo nextest`
/// guard, mirroring `render_config_emits_parseable_toml`.
fn rewrite_rate_per_mb(config: &str, rate: u64) -> anyhow::Result<String> {
    let mut doc: toml::Table = config.parse().context("parse node config")?;
    let payment = doc
        .get_mut("payment")
        .and_then(toml::Value::as_table_mut)
        .ok_or_else(|| anyhow::anyhow!("node config has no [payment] table"))?;
    payment.insert(
        "rate_per_mb".to_string(),
        toml::Value::Integer(i64::try_from(rate).context("rate_per_mb overflows i64")?),
    );
    toml::to_string(&doc).context("render node config")
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

/// Sibling suffix of a filesystem origin's pre-order outboard. Mirrors the
/// `pub(super)` `OBAO4_SUFFIX` in `decdn_cache::origin::fs`, kept in sync by the
/// `FilesystemOrigin::fetch_range` range-tier assertions the e2e journeys run.
const OBAO4_SUFFIX: &str = ".obao4";

/// Write the sibling `{hex}.obao4` pre-order outboard next to the data object.
/// Mirrors `decdn_cache`'s test-only `seed_blob_with_outboard`.
fn write_fs_origin_outboard(
    root: &std::path::Path,
    hash: &Hash,
    blob: &[u8],
) -> anyhow::Result<()> {
    use bao_tree::io::outboard::PreOrderMemOutboard;
    let ob = PreOrderMemOutboard::create(blob, decdn_bao_range::IROH_BLOCK_SIZE);
    let hex = hash.to_hex();
    let shard = hex.as_str().get(..2).context("blob hex too short")?;
    let dir = root.join(shard);
    std::fs::create_dir_all(&dir).context("create origin shard dir")?;
    std::fs::write(dir.join(format!("{}{OBAO4_SUFFIX}", hex.as_str())), ob.data)
        .context("write origin outboard")?;
    Ok(())
}

/// Locate the built `decdn-node` binary relative to the current test executable
/// (`target/<profile>/decdn-node`), falling back to `DECDN_NODE_BIN`.
fn decdn_node_bin() -> anyhow::Result<PathBuf> {
    let overridden = std::env::var_os("DECDN_NODE_BIN");
    let bin = if let Some(p) = &overridden {
        // Tilde-expanded like `cli::decdn_cli_bin`, matching how the production
        // CLI treats every user-supplied path.
        decdn_common::cli::common::expand_tilde(std::path::Path::new(p))
    } else {
        let exe = std::env::current_exe().context("current_exe")?;
        // .../target/<profile>/deps/<test-bin>  → .../target/<profile>/decdn-node
        let profile_dir = exe
            .parent()
            .and_then(|deps| deps.parent())
            .context("resolve target profile dir")?;
        profile_dir.join(if cfg!(windows) {
            "decdn-node.exe"
        } else {
            "decdn-node"
        })
    };
    // Check both branches, so a stale `DECDN_NODE_BIN` fails here rather than as
    // a bare "No such file or directory" at spawn time, and report the path
    // absolute since a relative override resolves against the crate root
    // (mirrors `cli::decdn_cli_bin`).
    anyhow::ensure!(
        bin.exists(),
        "decdn-node binary not found at {}{}",
        std::path::absolute(&bin)
            .unwrap_or_else(|_| bin.clone())
            .display(),
        if overridden.is_some() {
            " (from DECDN_NODE_BIN — stale or misspelled?)"
        } else {
            "; run `cargo build -p decdn-node` first (or set DECDN_NODE_BIN)"
        }
    );
    Ok(bin)
}

/// Spawn `decdn-node run --config <config_path>` with the fixture's test
/// keystore password and log level, hermetically. Shared by
/// [`NodeFixture::launch`] and [`NodeFixture::restart`].
///
/// `home` is taken explicitly rather than derived from `config_path`'s parent:
/// the two coincide today only because the config is written *inside* the data
/// dir, and `Path::parent` answers `Some("")` for a bare filename — an empty
/// `HOME` that `dirs` resolves back to the developer's real home. The daemon's
/// explicit `--config` already keeps it off `~/.decdn/node.toml`; the isolation
/// here covers the `DECDN_*` env namespace (which outranks the config) and any
/// path the config leaves to a `$HOME`-derived default (#1332).
fn spawn_daemon(config_path: &std::path::Path, home: &std::path::Path) -> anyhow::Result<Child> {
    crate::cli::hermetic_command(decdn_node_bin()?, home, KEYSTORE_PASSWORD)?
        .arg("--config")
        .arg(config_path)
        .arg("run")
        .spawn()
        .context("spawn decdn-node")
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    /// A malformed `render_config` template only surfaces behind the `anvil-e2e`
    /// feature (Foundry + a built binary). Parse the rendered TOML here so a
    /// template regression (bad `format!`, dropped/mis-nested key) is caught in
    /// a normal `cargo nextest run -p decdn-e2e`, no chain required.
    #[test]
    fn render_config_emits_parseable_toml() {
        let addrs = ContractAddrs {
            capacity_bond: Address::from([0x11; 20]),
            payment_channel: Address::from([0x22; 20]),
            fee_router: Address::from([0x33; 20]),
            token: Address::from([0x44; 20]),
            slash_judge: Address::from([0x55; 20]),
            slash_appeal: Address::from([0x66; 20]),
            governor: Address::from([0x77; 20]),
            timelock: Address::from([0x88; 20]),
            publisher_registry: Address::from([0x99; 20]),
            origin_assignment: Address::from([0xAA; 20]),
            content_blacklist: Address::from([0xBB; 20]),
        };
        let discovery_peers = [(iroh::SecretKey::from_bytes(&[0xCC; 32]).public(), 4434)];
        let rendered = render_config(&RenderConfig {
            data_dir: PathBuf::from("/var/lib/decdn"),
            region: "US",
            bind_port: 4433,
            admin_port: 9944,
            metrics_port: 9100,
            rpc_url: "http://127.0.0.1:8545",
            keystore: std::path::Path::new("/var/lib/decdn/keystore.json"),
            cache_dir: std::path::Path::new("/var/lib/decdn/cache"),
            origin_dir: std::path::Path::new("/var/lib/decdn/origin"),
            chain_id: 31_337,
            addrs,
            node_to_node_pull_through: true,
            // Distinct value from `node_to_node_pull_through` so a template that
            // wrongly re-tied the two keys (the pre-#1376 bug) is caught here.
            require_authorized_origin: false,
            discovery_peers: &discovery_peers,
        });

        // The core check: the whole template parses as TOML.
        let doc: toml::Value =
            toml::from_str(&rendered).expect("render_config must emit valid TOML");

        // Guard the tables/keys the daemon depends on against silent drift.
        assert_eq!(doc["blockchain"]["chain_id"].as_integer(), Some(31_337));
        assert_eq!(
            doc["blockchain"]["capacity_bond_address"].as_str(),
            Some(addrs.capacity_bond.to_string().as_str())
        );
        assert_eq!(
            doc["blockchain"]["redeem_threshold_micro_usdc"].as_integer(),
            Some(10)
        );
        assert_eq!(doc["cache"]["origin"]["kind"].as_str(), Some("fs"));
        assert!(doc["cache"]["origin"].get("path").is_some());
        // The two knobs render independently (#1376): the fixture set them to
        // distinct values above, so this also guards against a regression that
        // re-ties `pull_through_require_authorized_origin` to
        // `node_to_node_pull_through_enabled`.
        assert_eq!(
            doc["cache"]["node_to_node_pull_through_enabled"].as_bool(),
            Some(true)
        );
        assert_eq!(
            doc["cache"]["pull_through_require_authorized_origin"].as_bool(),
            Some(false)
        );
        assert_eq!(
            doc["network"]["discovery"]["peers"]
                .as_table()
                .map(toml::Table::len),
            Some(1)
        );
        assert_eq!(doc["payment"]["rate_per_mb"].as_integer(), Some(10));
        assert_eq!(doc["observability"]["admin_port"].as_integer(), Some(9944));
        assert_eq!(
            doc["observability"]["metrics_port"].as_integer(),
            Some(9100)
        );
    }

    /// Build a representative rendered node config for the TOML-round-trip tests.
    fn sample_rendered_config() -> String {
        let addrs = ContractAddrs {
            capacity_bond: Address::from([0x11; 20]),
            payment_channel: Address::from([0x22; 20]),
            fee_router: Address::from([0x33; 20]),
            token: Address::from([0x44; 20]),
            slash_judge: Address::from([0x55; 20]),
            slash_appeal: Address::from([0x66; 20]),
            governor: Address::from([0x77; 20]),
            timelock: Address::from([0x88; 20]),
            publisher_registry: Address::from([0x99; 20]),
            origin_assignment: Address::from([0xAA; 20]),
            content_blacklist: Address::from([0xBB; 20]),
        };
        render_config(&RenderConfig {
            data_dir: PathBuf::from("/var/lib/decdn"),
            region: "US",
            bind_port: 4433,
            admin_port: 9944,
            metrics_port: 9100,
            rpc_url: "http://127.0.0.1:8545",
            keystore: std::path::Path::new("/var/lib/decdn/keystore.json"),
            cache_dir: std::path::Path::new("/var/lib/decdn/cache"),
            origin_dir: std::path::Path::new("/var/lib/decdn/origin"),
            chain_id: 31_337,
            addrs,
            node_to_node_pull_through: true,
            require_authorized_origin: true,
            discovery_peers: &[],
        })
    }

    /// `set_rate_per_mb`'s parse → mutate → serialize step (`rewrite_rate_per_mb`)
    /// must round-trip: the new rate lands and every other section survives. The
    /// `[cache]` block is the one at risk — it holds a scalar
    /// (`node_to_node_pull_through_enabled`) after a sub-table (`[cache.origin]`),
    /// the `ValueAfterTable` shape a `toml` serializer bug would mangle (#1378).
    #[test]
    fn set_rate_per_mb_round_trips_through_toml() {
        let rewritten = rewrite_rate_per_mb(&sample_rendered_config(), 4242)
            .expect("rewrite_rate_per_mb must succeed");
        let doc: toml::Value =
            toml::from_str(&rewritten).expect("rewritten config must be valid TOML");

        assert_eq!(doc["payment"]["rate_per_mb"].as_integer(), Some(4242));
        // Everything around the mutation is intact — notably the `[cache]`
        // scalar-after-subtable that trips the serializer hazard.
        assert_eq!(doc["cache"]["origin"]["kind"].as_str(), Some("fs"));
        assert_eq!(
            doc["cache"]["node_to_node_pull_through_enabled"].as_bool(),
            Some(true)
        );
        assert_eq!(
            doc["cache"]["pull_through_require_authorized_origin"].as_bool(),
            Some(true)
        );
        assert_eq!(doc["blockchain"]["chain_id"].as_integer(), Some(31_337));
    }
}
