//! Live anvil-backed e2e that boots the **real** `decdn_node::runtime::run`
//! end-to-end and shuts it down gracefully via the admin drain RPC.
//!
//! Refactor #1253 decomposed `runtime::run` into phase functions
//! (`build_infra` → `build_chain_and_handlers` → `spawn_background_tasks` →
//! `serve_until_shutdown` → `shutdown`). Every in-crate unit test exercises a
//! phase (or a helper) in isolation; none drives the *assembled* runtime. This
//! smoke test is the only one that boots the whole thing against a live chain,
//! reaches readiness, and then walks the load-bearing teardown order in
//! `shutdown` (the `ShutdownHandles` exhaustive-destructure sequence) all the
//! way to a clean `Ok(())` return.
//!
//! ## Shape
//!
//! 1. Spawn a local `anvil`, deploy a mintable mock USDC, then deploy the full
//!    protocol via the production `forge script DeployProtocol.s.sol` (reading
//!    the deployed addresses from the `deployments/<chainId>.json` manifest) —
//!    mirroring `anvil_settlement_e2e.rs`.
//! 2. Generate a node Ethereum keystore, build a `ResolvedConfig` pointed at the
//!    anvil RPC and the deployed contract addresses, with data dirs under
//!    `TempDir`s and explicit free TCP ports for the admin + metrics listeners.
//! 3. `tokio::spawn(decdn_node::runtime::run(cfg, None, reload_state))`.
//! 4. Poll `admin_v1_health` on the configured admin port until the runtime is
//!    up (bounded retry loop), reusing the same jsonrpsee `AdminRpcClient` the
//!    `decdn node …` CLI uses.
//! 5. Trigger graceful shutdown via `admin_v1_drain` (the `decdn node drain`
//!    path) — NOT a process signal, which would hit the test harness.
//! 6. Await the runtime task and assert it resolves to `Ok(Ok(()))`: `run()`
//!    returned `Ok(())` (clean graceful shutdown), the task did not panic, and
//!    it did not time out.
//!
//! The node operator is intentionally **not** staked/registered on-chain: the
//! runtime's chain bring-up (`CapacityBond` registry bootstrap, the settlement
//! and buyer service bootstraps) is read-only or best-effort, so an unregistered
//! operator boots to readiness. See the report accompanying the PR for the
//! confirmation that stake/`registerNode` is unnecessary for bring-up.
//!
//! Gated behind the `anvil-e2e` feature and requires `anvil` + `forge` on
//! `PATH` (the CI job provisions Foundry). If they are absent the test fails
//! loudly rather than silently skipping — it is opt-in via the feature.

#![cfg(feature = "anvil-e2e")]
// Test-harness scaffolding legitimately uses unwrap/expect/panic and indexing
// on known-shape JSON; the workspace anti-panic policy targets runtime code.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::too_many_lines,
    clippy::cognitive_complexity,
    // Second-scale timeouts read more clearly as `from_secs` than `from_mins`.
    clippy::duration_suboptimal_units
)]

use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::Duration;

use alloy::network::{EthereumWallet, TransactionBuilder};
use alloy::primitives::{Address, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use decdn_common::admin::{AdminRpcClient, DrainRequest, HealthResponse};
use decdn_common::cli::run::{ObservabilityArgs, PaymentArgs};
use decdn_common::config::{
    ResolvedBlockchain, ResolvedCache, ResolvedConfig, ResolvedDht, ResolvedDiscovery,
    ResolvedGossip, ResolvedIdentity, ResolvedNetwork, ResolvedObservability, ResolvedPayment,
    ResolvedProbe, ResolvedReceipts, ResolvedSecurity,
};
use decdn_incentive::eth_identity;
use decdn_node::runtime::{LogLevelSetter, RuntimeReloadState};
use jsonrpsee::http_client::HttpClientBuilder;

// Test-only chain id in the gitignored `deployments/31337[67]*.json` band so the
// forge-script manifest never collides with a real chain's manifest (or the
// sibling `anvil_settlement_e2e.rs`, which uses 31_337_690).
const CHAIN_ID: u64 = 31_337_691;
// Default anvil dev account #0 (mnemonic "test test … junk") — funded at
// genesis, broadcasts the deploy. Not the forge-default sender the script rejects.
const DEPLOYER_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
const DEPLOYER_ADDR: &str = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266";
// Default anvil dev account #1 — the in-process "admin" EOA (deploys the mock
// USDC, funds gas). Deliberately NOT account #0: that EOA is the `forge script`
// broadcaster whose on-chain nonce the script advances by ~25; sharing it would
// desync alloy's cached nonce ("nonce too low").
const ADMIN_KEY: &str = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d";

// Wall-clock bounds on the `forge` subprocesses. `forge build` compiles the
// contract set cold; the deploy normally finishes in ~1–5s but intermittently
// stalls/hiccups under runner CPU contention — bounded per attempt and retried.
const FORGE_BUILD_TIMEOUT: Duration = Duration::from_secs(180);
const DEPLOY_TIMEOUT: Duration = Duration::from_secs(45);
const DEPLOY_ATTEMPTS: usize = 3;

// Bring-up readiness budget: how long to poll `admin_v1_health` for the admin
// server to answer. Generous — a cold chain bring-up (RPC preflight + keystore
// decrypt + blacklist initial sync) can take a few seconds on a loaded runner.
const READY_TIMEOUT: Duration = Duration::from_secs(30);
// Await budget for `run()` to return after drain. The runtime's internal
// `SHUTDOWN_DEADLINE` is 15s; 90s leaves comfortable margin for a loaded runner
// while still failing fast on a genuinely wedged teardown.
const SHUTDOWN_AWAIT: Duration = Duration::from_secs(90);

/// Kills the spawned `anvil` on drop so a panicking assertion never leaks the
/// process, and removes the (gitignored) deploy manifest so re-runs start clean.
struct AnvilGuard {
    child: Child,
    manifest: PathBuf,
}

impl Drop for AnvilGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(&self.manifest);
    }
}

fn contracts_dir() -> PathBuf {
    // crates/node/ → ../../contracts
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../contracts")
        .canonicalize()
        .expect("contracts dir")
}

/// Grab an ephemeral TCP port, then release it for the caller to claim.
fn free_port() -> u16 {
    let l = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("bind ephemeral");
    l.local_addr().expect("local_addr").port()
}

/// Poll `f` until it yields `Some`, or `timeout` elapses.
async fn poll_until<T, F, Fut>(timeout: Duration, mut f: F) -> Option<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(v) = f().await {
            return Some(v);
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// Run a `forge` subprocess to completion under a wall-clock `timeout`, killing
/// the child if it overruns (`kill_on_drop` SIGKILLs the otherwise-orphaned
/// child when the timed-out future is dropped). Three outcomes are kept
/// distinct: `Err(_)` (spawn failed — deterministic, fail fast), `Ok(Err(_))`
/// (stalled and killed — retryable), `Ok(Ok(output))` (exited — inspect status).
async fn forge_output(
    mut cmd: tokio::process::Command,
    timeout: Duration,
    what: &str,
) -> anyhow::Result<Result<std::process::Output, Duration>> {
    cmd.kill_on_drop(true);
    match tokio::time::timeout(timeout, cmd.output()).await {
        Ok(res) => res
            .map(Ok)
            .with_context(|| format!("spawn `{what}` (is foundry installed?)")),
        Err(_) => Ok(Err(timeout)),
    }
}

/// `true` when a non-zero `forge script` exit happened *after* the script body
/// completed (`Script ran successfully` printed) — a transient broadcast-phase
/// hiccup worth retrying. `false` (marker absent) means a deterministic
/// revert/script bug that should fail fast.
fn forge_script_body_completed(stdout: &[u8]) -> bool {
    String::from_utf8_lossy(stdout).contains("Script ran successfully")
}

/// Deploy the mintable mock USDC from its compiled artifact bytecode.
async fn deploy_mock_usdc<P: Provider>(provider: &P, contracts: &Path) -> anyhow::Result<Address> {
    let artifact = contracts.join("out/MintableUSDC.sol/MintableUSDC.json");
    let bytes = std::fs::read(&artifact).map_err(|e| {
        anyhow::anyhow!("read MintableUSDC artifact at {}: {e}", artifact.display())
    })?;
    let json: serde_json::Value = serde_json::from_slice(&bytes)?;
    let code_hex = json["bytecode"]["object"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("MintableUSDC artifact missing bytecode.object"))?;
    let code: alloy::primitives::Bytes = code_hex.parse()?;
    let receipt = provider
        .send_transaction(alloy::rpc::types::TransactionRequest::default().with_deploy_code(code))
        .await?
        .get_receipt()
        .await?;
    receipt
        .contract_address
        .ok_or_else(|| anyhow::anyhow!("MintableUSDC deploy produced no contract address"))
}

/// Run `forge script DeployProtocol.s.sol` against the anvil RPC, broadcasting
/// from the anvil dev deployer. Retries the two transient failure classes
/// (receipt-wait stall #785, broadcast-phase non-zero exit #883); a revert
/// before the body completes fails fast.
async fn run_deploy_script(
    contracts: &Path,
    rpc_url: &str,
    usdc: Address,
    initial_token_holder: Address,
) -> anyhow::Result<()> {
    for attempt in 1..=DEPLOY_ATTEMPTS {
        let mut cmd = tokio::process::Command::new("forge");
        cmd.current_dir(contracts)
            .args([
                "script",
                "script/DeployProtocol.s.sol:DeployProtocol",
                "--rpc-url",
                rpc_url,
                "--broadcast",
                "--private-key",
                DEPLOYER_KEY,
                "--sender",
                DEPLOYER_ADDR,
            ])
            .env("USDC_ADDRESS", usdc.to_string())
            .env("INITIAL_TOKEN_HOLDER", initial_token_holder.to_string())
            .env("EMERGENCY_MULTISIG", DEPLOYER_ADDR)
            // ADR 019 § Terms Acceptance — DeployProtocol requires a non-zero
            // genesis terms hash (CapacityBond rejects the zero sentinel).
            .env(
                "CURRENT_TERMS_HASH",
                "0x0000000000000000000000000000000000000000000000000000000000000001",
            )
            .env("FORCE_OVERWRITE_MANIFEST", "true");
        match forge_output(cmd, DEPLOY_TIMEOUT, "forge script DeployProtocol").await? {
            Ok(out) if out.status.success() => return Ok(()),
            Ok(out) if forge_script_body_completed(&out.stdout) => {
                if attempt == DEPLOY_ATTEMPTS {
                    anyhow::bail!(
                        "forge script DeployProtocol failed after {DEPLOY_ATTEMPTS} attempts; the final attempt completed the body but exited non-zero during broadcast:\n{}\n{}",
                        String::from_utf8_lossy(&out.stdout),
                        String::from_utf8_lossy(&out.stderr)
                    );
                }
                tracing::warn!(
                    "forge script DeployProtocol attempt {attempt}/{DEPLOY_ATTEMPTS} completed the body but exited non-zero during broadcast (transient), retrying"
                );
            }
            Ok(out) => anyhow::bail!(
                "forge script DeployProtocol exited non-zero before the body completed (revert or script bug):\n{}\n{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            ),
            Err(timeout) => {
                if attempt == DEPLOY_ATTEMPTS {
                    anyhow::bail!(
                        "forge script DeployProtocol failed after {DEPLOY_ATTEMPTS} attempts; the final attempt stalled (timed out after {timeout:?})"
                    );
                }
                tracing::warn!(
                    "forge script DeployProtocol attempt {attempt}/{DEPLOY_ATTEMPTS} stalled (killed after {timeout:?}), retrying"
                );
            }
        }
    }
    anyhow::bail!("DEPLOY_ATTEMPTS must be >= 1 (was {DEPLOY_ATTEMPTS})")
}

/// The deployed contract addresses this test threads into `ResolvedConfig`.
struct DeployedAddrs {
    payment_channel: Address,
    capacity_bond: Address,
    slash_judge: Address,
    content_blacklist: Address,
}

/// Read the four contract addresses the node runtime needs from the deploy
/// manifest (`deployments/<chainId>.json`, `contracts` map).
fn read_manifest(path: &Path) -> anyhow::Result<DeployedAddrs> {
    let json: serde_json::Value = serde_json::from_slice(&std::fs::read(path)?)?;
    let c = &json["contracts"];
    let get = |k: &str| -> anyhow::Result<Address> {
        Ok(c[k]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("manifest missing contracts.{k}"))?
            .parse()?)
    };
    Ok(DeployedAddrs {
        payment_channel: get("PaymentChannel")?,
        capacity_bond: get("CapacityBond")?,
        slash_judge: get("SlashJudge")?,
        content_blacklist: get("ContentBlacklist")?,
    })
}

/// Build a `ResolvedConfig` pointed at the anvil RPC + deployed contracts, with
/// the given data/cache dirs, keystore, password file, and explicit listener
/// ports. Mirrors the exhaustive fixture shape in `runtime`'s own tests and in
/// `sighup_signal.rs`; only the chain/identity/port fields are load-bearing here.
#[allow(clippy::too_many_arguments)]
fn build_config(
    data_dir: PathBuf,
    cache_dir: PathBuf,
    keystore_path: PathBuf,
    password_file: PathBuf,
    rpc_url: String,
    addrs: &DeployedAddrs,
    admin_port: u16,
    metrics_port: u16,
) -> ResolvedConfig {
    ResolvedConfig {
        identity: ResolvedIdentity {
            data_dir,
            region: None,
        },
        network: ResolvedNetwork {
            // Ephemeral iroh QUIC bind port — nothing dials this node.
            bind_port: 0,
            relay_urls: Vec::new(),
            discovery: ResolvedDiscovery::default(),
        },
        blockchain: ResolvedBlockchain {
            origin_assignment_address: None,
            publisher_registry_address: None,
            origin_directory_from_block: 0,
            rpc_url,
            eth_keystore: keystore_path,
            keystore_password_file: Some(password_file),
            payment_channel_address: addrs.payment_channel.to_string(),
            capacity_bond_address: addrs.capacity_bond.to_string(),
            rpc_watchdog_interval_sec: 30,
            event_poll_interval_ms: 250,
            rate_bounds_poll_interval_sec: 3600,
            redeem_threshold_micro_usdc: 1_000_000,
            buyer_deposit_micro_usdc: 10_000_000,
            // No on-chain approval tx at bring-up: keeps the (unregistered,
            // gas-funded-but-otherwise-inert) node's boot path chain-write-free.
            buyer_max_approve: false,
            settlement_auto_threshold_micro_usdc: None,
            settlement_auto_by_voucher_nonce_span: None,
            slash_judge_address: addrs.slash_judge.to_string(),
            slash_judge_from_block: 0,
            content_blacklist_address: Some(addrs.content_blacklist.to_string()),
            content_blacklist_from_block: 0,
            content_blacklist_poll_interval_sec: 600,
            chain_id: CHAIN_ID,
        },
        cache: ResolvedCache {
            cache_dir,
            cache_size_mb: 1024,
            max_blob_size_mb: 128,
            max_rate_per_mb: 0,
            origins: Vec::new(),
            pinned_hashes: decdn_cache::PinnedHashes::empty(),
            origin_retry: decdn_cache::RetryPolicy::default(),
            circuit_breaker: decdn_cache::CircuitBreakerPolicy::default(),
            user_agent: decdn_cache::DEFAULT_USER_AGENT.to_string(),
            gc_interval_sec: 0,
            eviction_high_water_pct: 90,
            eviction_target_pct: 80,
            eviction_per_sweep_budget: 16,
            eviction_tick_secs: 1,
            max_probe_holds: decdn_common::config::DEFAULT_MAX_PROBE_HOLDS,
            stake_lane_reserved_holds: decdn_common::config::DEFAULT_STAKE_LANE_RESERVED_HOLDS,
            node_to_node_pull_through_enabled: false,
            node_pull_probe_fanout: decdn_common::config::DEFAULT_NODE_PULL_PROBE_FANOUT,
            node_pull_timeout_sec: decdn_common::config::DEFAULT_NODE_PULL_TIMEOUT_SEC,
            node_pull_stall_timeout_sec: decdn_common::config::DEFAULT_NODE_PULL_STALL_TIMEOUT_SEC,
            pull_ahead_bytes: decdn_cache::Bytes::new(
                decdn_common::config::DEFAULT_PULL_AHEAD_BYTES,
            ),
            max_unrecouped_leech_bytes: decdn_cache::Bytes::new(
                decdn_common::config::DEFAULT_MAX_UNRECOUPED_LEECH_BYTES,
            ),
            pull_share_ratio_percent: decdn_cache::Percent::new(
                decdn_common::config::DEFAULT_PULL_SHARE_RATIO_PERCENT,
            ),
            pull_through_require_authorized_origin: false,
        },
        payment: ResolvedPayment {
            rate_per_mb: 10,
            delivery_floor: 0,
            voucher_interval_mb: decdn_protocol::DEFAULT_VOUCHER_INTERVAL_MB,
            credit_window_bytes: decdn_common::config::DEFAULT_CREDIT_WINDOW_BYTES,
        },
        observability: ResolvedObservability {
            log_level: decdn_common::cli::common::LogLevel::Info,
            log_format: decdn_common::cli::common::LogFormat::Pretty,
            metrics_port,
            metrics_bind: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            admin_port: Some(admin_port),
            otlp_endpoint: None,
            region_accounting_interval_sec:
                decdn_common::config::DEFAULT_REGION_ACCOUNTING_INTERVAL_SEC,
        },
        gossip: ResolvedGossip {
            announce_interval_sec: 60,
            peer_ttl_sec: 600,
            subscribe_global: false,
            max_peer_entries: Some(100_000),
        },
        security: ResolvedSecurity {
            max_concurrent_handlers: 256,
            per_source_rate_per_sec: 100.0,
            per_source_burst: 200,
            max_tracked_sources: 4096,
        },
        content: decdn_common::config::ResolvedContent::default(),
        dht: ResolvedDht::default(),
        probe: ResolvedProbe::default(),
        receipts: ResolvedReceipts::default(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn anvil_bringup_shutdown_runtime_graceful_drain() -> anyhow::Result<()> {
    // Surface the runtime's own bring-up/teardown logs on failure. Those tasks
    // run on tokio worker threads under `multi_thread`, so write to process
    // stderr (nextest captures it and shows it on failure) rather than
    // `with_test_writer` (thread-local, main-thread only). `try_init` is
    // idempotent (harmless on a re-run).
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let contracts = contracts_dir();

    // ---- 0. Build contracts so the MintableUSDC artifact + deploy script exist.
    let mut build_cmd = tokio::process::Command::new("forge");
    build_cmd.current_dir(&contracts).args(["build"]);
    let build = match forge_output(build_cmd, FORGE_BUILD_TIMEOUT, "forge build").await? {
        Ok(out) => out,
        Err(timeout) => anyhow::bail!("`forge build` timed out after {timeout:?}"),
    };
    assert!(
        build.status.success(),
        // forge writes compiler errors to stdout, not stderr — capture both.
        "forge build failed:\n{}\n{}",
        String::from_utf8_lossy(&build.stdout),
        String::from_utf8_lossy(&build.stderr)
    );

    // ---- 1. Spawn anvil.
    let anvil_port = free_port();
    let rpc_url = format!("http://127.0.0.1:{anvil_port}");
    let child = Command::new("anvil")
        .args([
            "--port",
            &anvil_port.to_string(),
            "--chain-id",
            &CHAIN_ID.to_string(),
            "--silent",
        ])
        .spawn()
        .expect("spawn anvil (is foundry installed?)");
    let manifest = contracts.join(format!("deployments/{CHAIN_ID}.json"));
    let _anvil = AnvilGuard {
        child,
        manifest: manifest.clone(),
    };

    let url: reqwest::Url = rpc_url.parse()?;
    let admin_signer: PrivateKeySigner = ADMIN_KEY.parse()?;
    let admin = ProviderBuilder::new()
        .wallet(EthereumWallet::from(admin_signer))
        .connect_http(url.clone());

    // Wait for the RPC to accept requests.
    poll_until(Duration::from_secs(20), || async {
        admin.get_chain_id().await.ok()
    })
    .await
    .expect("anvil RPC never came up");

    // ---- 2. Node Ethereum keystore. The node data dir also holds the iroh
    // `node.secret` (auto-generated by the runtime) and the redb channel store,
    // so it must exist with `0o700` before either the keystore generation or the
    // runtime's `ensure_data_dir` validation.
    let data_tmp = tempfile::tempdir()?;
    let cache_tmp = tempfile::tempdir()?;
    let pw_tmp = tempfile::tempdir()?;
    #[cfg(unix)]
    std::fs::set_permissions(
        data_tmp.path(),
        std::os::unix::fs::PermissionsExt::from_mode(0o700),
    )?;
    let password = "e2e-bringup-shutdown-password";
    let password_file = pw_tmp.path().join("keystore.pass");
    std::fs::write(&password_file, password)?;
    let eth_addr = eth_identity::generate_and_persist(data_tmp.path(), password, false)?;
    let keystore_path = eth_identity::keystore_path(data_tmp.path());

    // Fund the node EOA with gas so any best-effort chain write during bring-up
    // (there are none with `buyer_max_approve=false`, but this is cheap
    // insurance) has ETH. Not staked/registered — bring-up does not require it.
    let _: serde_json::Value = admin
        .raw_request(
            "anvil_setBalance".into(),
            (
                eth_addr,
                U256::from(100u64) * U256::from(10u64).pow(U256::from(18)),
            ),
        )
        .await?;

    // ---- 3. Deploy mock USDC then the production protocol.
    let usdc_addr = deploy_mock_usdc(&admin, &contracts).await?;
    run_deploy_script(&contracts, &rpc_url, usdc_addr, eth_addr).await?;
    let addrs = read_manifest(&manifest)?;

    // ---- 4. Build the resolved config with explicit free listener ports.
    let admin_port = free_port();
    let metrics_port = free_port();
    let cfg = build_config(
        data_tmp.path().to_path_buf(),
        cache_tmp.path().to_path_buf(),
        keystore_path,
        password_file,
        rpc_url,
        &addrs,
        admin_port,
        metrics_port,
    );

    // The reload state `commands::run` builds before handing off to
    // `runtime::run`. No CLI overrides, and a no-op log-level setter (SIGHUP
    // reload is out of scope for this smoke test).
    let reload_state = Arc::new(RuntimeReloadState::new(
        PaymentArgs {
            rate_per_mb: None,
            delivery_floor: None,
        },
        ObservabilityArgs {
            log_level: None,
            log_format: None,
            metrics_port: None,
            metrics_bind: None,
            admin_port: None,
            otlp_endpoint: None,
        },
        &cfg,
        Box::new(|_| Ok(())) as LogLevelSetter,
    ));

    // ---- 5. Boot the real runtime.
    let handle = tokio::spawn(decdn_node::runtime::run(cfg, None, reload_state));

    // ---- 6. Poll the admin health endpoint until the runtime answers — the
    // readiness signal (the admin server binds and serves once bring-up has
    // progressed through infra + chain + background-task assembly). Reuse the
    // jsonrpsee `AdminRpcClient` the `decdn node health/drain` CLI uses.
    let admin_url = format!("http://127.0.0.1:{admin_port}");
    let client = HttpClientBuilder::default()
        .request_timeout(Duration::from_secs(5))
        .build(&admin_url)
        .with_context(|| format!("failed to build admin JSON-RPC client for {admin_url}"))?;

    let health: Option<HealthResponse> = poll_until(READY_TIMEOUT, || {
        let client = &client;
        async move { client.health().await.ok() }
    })
    .await;
    // If the runtime task already failed, surface *its* error rather than a bare
    // "never became ready" — the task error is the actionable diagnostic.
    if health.is_none() {
        if handle.is_finished() {
            match handle.await {
                Ok(Ok(())) => anyhow::bail!(
                    "runtime returned Ok(()) before the admin server ever answered health"
                ),
                Ok(Err(e)) => {
                    return Err(e).context("runtime exited with an error during bring-up");
                }
                Err(join) => return Err(anyhow::anyhow!("runtime task panicked: {join}")),
            }
        }
        anyhow::bail!("admin health never became ready within {READY_TIMEOUT:?}");
    }

    // ---- 7. Trigger graceful shutdown via `admin_v1_drain` (fire-and-forget,
    // the `decdn node drain` path). NOT a process signal — that would hit the
    // test harness. `wait_admin=false` returns as soon as the drain trigger is
    // queued; the runtime then walks its teardown and `run()` returns.
    let drain = client
        .drain(Some(DrainRequest { wait_admin: false }))
        .await
        .context("admin_v1_drain call failed")?;
    anyhow::ensure!(drain.initiated, "drain must report initiated=true");

    // ---- 8. Await a clean graceful exit: no timeout, no panic, Ok(()) return.
    let outcome = tokio::time::timeout(SHUTDOWN_AWAIT, handle)
        .await
        .with_context(|| {
            format!("runtime did not shut down within {SHUTDOWN_AWAIT:?} after drain")
        })?;
    match outcome {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e).context("runtime returned an error from graceful shutdown"),
        Err(join) => Err(anyhow::anyhow!(
            "runtime task panicked during shutdown: {join}"
        )),
    }
}
