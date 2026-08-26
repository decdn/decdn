//! Live anvil-backed e2e for the on-chain `PaymentPool` settlement path (ADR
//! 003): the seller redemption loop and the buyer open/top-up/reclaim loop, both
//! driven against a real chain. Gated behind the `anvil-e2e` feature so the
//! default test run stays fast and needs no `anvil`/`forge` binaries.
//!
//! A pool is owner-owned and owner-closed. The node is the *provider* (payee) on
//! its voucher lanes: its only settlement primitive is `redeem` (there is no
//! close, dispute, or settle on the seller side). The buyer half owns one pool,
//! signs capability vouchers against its `(signer, provider)` lanes, and reclaims
//! the residual after a grace-window close. This test exercises both halves end
//! to end on a live deployment:
//!
//!   1. **Genuine voucher acceptance + redemption.** A buyer opens a pool, presents
//!      its self-signed capability on a real `cdn/client/v1` paid-delivery stream,
//!      and the serve handler registers a lane from that capability and persists a
//!      genuine client-signed voucher. The seller redeemer then submits `redeem`;
//!      the contract's `ECDSA.recover` accepts the normalized voucher signature,
//!      `getWatermark` advances, and `FeeRouter.bytesPerEpoch` increments.
//!   2. **Live event decode.** The [`PoolSettlementService`] watcher's
//!      subscribe+decode path runs against a live RPC (`PoolRedeemed` drives the
//!      paid watermark cache that gates redemption).
//!   3. **Buyer lifecycle.** A [`BuyerPoolService`] opens a pool, delivers a paid
//!      stream signed via the service-produced [`PoolContext`], tops the pool up,
//!      and — after an owner `closePool` and a grace-window warp — reclaims the
//!      residual, refunding the deposit and dropping the local record.
//!
//! ## Shape
//!
//! 1. Spawn a local `anvil`, deploy a mintable mock USDC, then deploy the full
//!    protocol via the production `forge script DeployProtocol.s.sol` (reading the
//!    deployed addresses from the `deployments/<chainId>.json` manifest).
//! 2. Activate the node operator on-chain: `stake` + `registerNode` (the iroh
//!    ed25519 node key signs the ownership proof the production `Ed25519Verifier`
//!    checks; the eth key signs the EIP-712 binding).
//! 3. Bring up the seller settlement service in-process
//!    ([`PoolSettlementService::bootstrap`] + a [`PersistentPoolStateStore`] + a
//!    [`ClientHandler`] wired with a `capability_sink` and a chain `pool_view`)
//!    pointed at the anvil RPC.
//! 4. Seller path — a buyer opens a pool on-chain, runs a real iroh paid-delivery
//!    roundtrip carrying its self-capability so a genuine voucher is produced, and
//!    the redeemer's `redeem` lands: `getWatermark` advances and
//!    `FeeRouter.bytesPerEpoch` increments.
//! 5. Buyer path — a [`BuyerPoolService`] opens a pool, delivers over the
//!    service-produced context, tops the pool up, then closes + reclaims after the
//!    grace window, asserting the full deposit refunds and the record drops.
//!
//! Requires `anvil` + `forge` on `PATH` (the CI job provisions Foundry). If they
//! are absent the test fails loudly rather than silently skipping — it is opt-in
//! via the feature, so reaching it means the caller asked for it.

#![cfg(feature = "anvil-e2e")]
// Test-harness scaffolding legitimately uses unwrap/expect/panic and indexing
// on known-shape JSON; the workspace anti-panic policy targets runtime code.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    // Second-scale timeouts read more clearly as `from_secs` than `from_mins`.
    clippy::duration_suboptimal_units
)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use alloy::network::{EthereumWallet, TransactionBuilder};
use alloy::primitives::{Address, B256, Bytes, U256, keccak256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::types::Filter;
use alloy::signers::SignerSync;
use alloy::signers::local::PrivateKeySigner;
use alloy::sol_types::SolEvent;
use anyhow::Context;
use decdn_client_pull::buyer_pool::{SELF_CAPABILITY_CAP, issue_self_capability};
use decdn_client_pull::sign_client_binding;
use decdn_incentive::payment_pool::PaymentPool;
use decdn_incentive::{
    BuyerPoolStore, KeyedCheckpointStore, MemoryBuyerPoolStore, PoolStateStore,
    bind_node_id_domain, register_node_signing_hash, slash_judge_domain, voucher_domain,
};
use decdn_node::buyer_channel::BuyerPoolService;
use decdn_node::chain_events::shared_head::{HeadSource, SharedHead};
use decdn_node::channel_store::{PersistentPoolStateStore, StoredCapabilitySource};
use decdn_node::client_requester::{PoolContext, stream_fetch};
use decdn_node::metrics::Metrics;
use decdn_node::payment_settlement::PoolSettlementService;
use decdn_protocol::ALPN_CLIENT;
use iroh::EndpointAddr;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

mod support;
use support::{
    HandlerDomains, build_handler_full_configured, cache_with_blob, fresh_key, local_endpoint,
    permissive_limiter, spawn_server,
};

/// Caller budget for `open_or_reuse_pool` in these tests (#1143). Generous: anvil
/// mines instantly, and the budget bounds how long a CALLER waits, not how long
/// the open may take — a tight value here would make the tests flaky on a loaded
/// runner without testing anything the unit tests don't already cover.
const OPEN_BUDGET: Duration = Duration::from_secs(60);

// Test-only chain id in the `deployments/3133769*.json` gitignore range so the
// forge-script manifest never collides with (or is committed alongside) a real
// chain's manifest.
const CHAIN_ID: u64 = 31_337_690;
// Default anvil dev account #0 (mnemonic "test test … junk") — funded with ETH
// at genesis, used to broadcast the deploy. Not the forge-default sender the
// deploy script rejects.
const DEPLOYER_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
const DEPLOYER_ADDR: &str = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266";
// Default anvil dev account #1 — the in-process "admin" EOA (deploys the mock
// USDC, mints, funds gas). Deliberately NOT account #0: that EOA is the `forge
// script` broadcaster, whose on-chain nonce the script advances by ~25; sharing
// it would desync alloy's cached nonce ("nonce too low").
const ADMIN_KEY: &str = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d";

const EPOCH_LENGTH_SECS: u64 = 7 * 24 * 60 * 60; // FeeRouter/CapacityBond constant
const RATE_PER_MB: u64 = 10; // µUSDC per MiB (within deliveryFloor/ceiling)
const MIB: usize = 1024 * 1024;

// The redeem threshold sits below the seller stream's ~15 µUSDC claim so the
// delivered voucher crosses it and the redeemer submits `redeem`.
const REDEEM_THRESHOLD_MICRO_USDC: u64 = 10;

const DEPOSIT_MICRO_USDC: u64 = 10_000_000; // 10 USDC (ADR 003 recommended minimum)
const TOPUP_MICRO_USDC: u64 = 2_000_000; // 2 USDC added via topUp in the buyer path
// Warp past any governable pool grace window so `reclaim` is permitted on-chain
// in the buyer-path reclaim assertion.
const GRACE_WINDOW_WARP_SECS: u64 = 60 * 24 * 60 * 60;

// Wall-clock bounds on the `forge` subprocesses (issue #785). `forge build`
// compiles the contract set cold; the deploy normally finishes in ~1–5s but
// `forge script --broadcast` intermittently fails under runner CPU contention in
// two transient ways, both bounded per attempt and retried: a *stall* (timeout in
// receipt-wait, #785) and a *broadcast-phase non-zero exit* (#883) where the
// script body completed but tx submission hit a nonce/RPC/anvil hiccup. A
// non-zero exit *before* the body completes (a genuine revert or script bug) is
// deterministic and fails fast. `DEPLOY_ATTEMPTS` is 3 so a single run can absorb
// one of each transient class (the #883 flake was stall-then-broadcast-hiccup)
// and still get a clean attempt.
const FORGE_BUILD_TIMEOUT: Duration = Duration::from_secs(180);
const DEPLOY_TIMEOUT: Duration = Duration::from_secs(45);
const DEPLOY_ATTEMPTS: usize = 3;
// Overall ceiling on the single e2e flow, sized above the sum of the internal
// `poll_until` budgets + build + deploy so a slow-but-legitimate run still
// surfaces its specific poll diagnostic, while a truly *unbounded* await (iroh,
// `get_receipt`) fails fast. Stays under the 15-minute CI job cap.
const OVERALL_TIMEOUT: Duration = Duration::from_secs(780);

/// A `tracing` [`Visit`](tracing::field::Visit) that pulls the `tx` and
/// `cap_count` fields off one event, ignoring everything else. Both fields
/// funnel through `record_debug` regardless of how they were logged (`%tx` is
/// a `Display`-wrapping `Debug` impl with no added quoting; a bare `cap_count`
/// falls back to `record_debug` via the `Visit` trait's default method
/// bodies), so this single override is sufficient.
#[derive(Default)]
struct CapCountVisitor {
    cap_count: Option<usize>,
    tx: Option<B256>,
}

impl tracing::field::Visit for CapCountVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        match field.name() {
            "cap_count" => self.cap_count = format!("{value:?}").parse().ok(),
            "tx" => self.tx = format!("{value:?}").parse().ok(),
            _ => {}
        }
    }
}

/// A `tracing_subscriber` [`Layer`](tracing_subscriber::Layer) that records the
/// redeemer's own `cap_count` — how many `CapabilityReg`s it attached to a
/// landed `redeemMany` — keyed by transaction hash, from the
/// `"batched lane redemption landed (redeemMany)"` info log in
/// `crates/node/src/payment_settlement.rs`. This is the direct,
/// deterministic proof the second-sweep-attaches-no-`CapabilityReg`
/// assertion in `run_e2e` needs: `contracts/src/PaymentPool.sol`'s
/// `_registerCapability` returns before its SSTORE/signature-check once
/// `cap != 0`, so a *resent* `CapabilityReg` is a near-gas-free no-op —
/// neither an on-chain event nor a gas-cost delta reliably distinguishes
/// "attached and no-op'd" from "never attached". Reading the redeemer's own
/// logged decision sidesteps that blind spot entirely.
struct CapCountLayer {
    captured: Arc<Mutex<HashMap<B256, usize>>>,
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CapCountLayer {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut visitor = CapCountVisitor::default();
        event.record(&mut visitor);
        if let (Some(cap_count), Some(tx)) = (visitor.cap_count, visitor.tx) {
            self.captured
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(tx, cap_count);
        }
    }
}

/// Kills the spawned `anvil` on drop so a panicking assertion never leaks the
/// process.
struct AnvilGuard {
    child: Child,
    manifest: PathBuf,
}

impl Drop for AnvilGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        // Manifest is gitignored, but remove it so re-runs start clean.
        let _ = std::fs::remove_file(&self.manifest);
    }
}

/// A real [`SharedHead`] over the anvil provider, matching the poll interval the
/// settlement service is bootstrapped with (so the TTL is 125 ms here, not the
/// 3.5 s a production 7 s interval yields).
fn e2e_head<P>(provider: &P) -> Arc<dyn HeadSource>
where
    P: Provider + Clone + 'static,
{
    Arc::new(SharedHead::new(
        provider.clone(),
        Duration::from_millis(250),
    ))
}

/// Current Unix time in seconds — the base for a generous capability expiry.
fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

fn contracts_dir() -> PathBuf {
    // crates/node/ → ../../contracts
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../contracts")
        .canonicalize()
        .expect("contracts dir")
}

/// Grab an ephemeral TCP port, then release it for anvil to claim.
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
/// the child if it overruns. `forge script --broadcast` intermittently stalls in
/// its broadcast/receipt-wait phase against anvil (issue #785); an unbounded
/// `std::process::Command::output()` would freeze the whole test. `kill_on_drop`
/// SIGKILLs the child (the otherwise-orphaned `forge`) when the timed-out
/// `output()` future is dropped; the tokio runtime then reaps it so it never
/// lingers as a zombie.
///
/// The three outcomes are kept distinct so callers can react correctly:
/// - `Err(_)` — the child could not be spawned (e.g. `forge` missing). This is
///   deterministic; callers should fail fast, not retry.
/// - `Ok(Err(timeout))` — the run stalled and was killed (#785). Retryable.
/// - `Ok(Ok(output))` — the process exited; the caller inspects its status.
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

/// Classifies a non-zero `forge script` exit (#883). Returns `true` when the
/// script *body* completed — i.e. simulation succeeded and forge printed its
/// `Script ran successfully` line — but the process still exited non-zero, which
/// means the failure was in the later broadcast / tx-submission phase (a
/// transient nonce/RPC/anvil hiccup under CPU contention, same root cause as the
/// #785 stall; retryable). Returns `false` when that marker is absent, i.e. the
/// script reverted or aborted before the body completed — a deterministic
/// revert/script bug that should fail fast with full output rather than burn
/// retries on a guaranteed-identical failure.
fn forge_script_body_completed(stdout: &[u8]) -> bool {
    String::from_utf8_lossy(stdout).contains("Script ran successfully")
}

#[test]
fn broadcast_phase_failure_is_retryable() {
    // Representative of the #883 attempt-2 stdout (line order condensed): the
    // body ran (simulation + `Return` printed), then the process cut off in the
    // broadcast/EVM-setup phase. Marker present ⇒ transient broadcast hiccup ⇒ retry.
    let stdout = b"No files changed, compilation skipped\nScript ran successfully.\n\n== Return ==\nd: struct BaseProtocolDeploy.Deployment Deployment({ token: 0x959, paymentPool: 0x4ed })\n\n## Setting up 1 EVM.";
    assert!(forge_script_body_completed(stdout));
}

#[test]
fn genuine_revert_fails_fast() {
    // A revert/abort during simulation never prints the success marker, so the
    // body did not complete ⇒ deterministic ⇒ fail fast (no retry).
    let stdout = b"Error: Simulated execution failed.\nReason: revert: ProviderNotActive\n";
    assert!(!forge_script_body_completed(stdout));
}

#[test]
fn empty_output_is_not_retryable() {
    assert!(!forge_script_body_completed(b""));
}

// Bindings for the setup/write calls not exposed by the production
// `decdn_incentive` bindings. The whole `PaymentPool` surface (`openPool`,
// `topUp`, `closePool`, `reclaim`, `getPool`, `getWatermark`, and every event) is
// reused from `decdn_incentive::payment_pool::PaymentPool` — the same ABI the
// runtime decodes — so there is a single source of truth for the pool layout.
// Only the ERC-20, operator-registration, and FeeRouter views are declared here.
alloy::sol! {
    #[sol(rpc)]
    contract Erc20 {
        function mint(address to, uint256 amount) external;
        function approve(address spender, uint256 amount) external returns (bool);
        function balanceOf(address account) external view returns (uint256);
    }

    #[sol(rpc)]
    contract CapacityBondWrite {
        function bond(uint256 amount) external;
        function currentTermsHash() external view returns (bytes32);
        function registerNode(
            bytes32 nodeId,
            bytes multiaddrs,
            string regionHint,
            bytes32 termsHash,
            bytes bindingSignature,
            bytes ed25519Signature
        ) external;
        function isActive(address operator) external view returns (bool);
    }

    #[sol(rpc)]
    contract FeeRouterView {
        function bytesPerEpoch(address operator, uint64 epoch) external view returns (uint256);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn e2e_onchain_payment_pool_settlement() -> anyhow::Result<()> {
    // Defense-in-depth: bound the whole flow so any unbounded await (iroh,
    // `get_receipt`) fails fast with a clear message instead of squatting the
    // runner. Cleanup is preserved on timeout — dropping `run_e2e`'s future runs
    // `AnvilGuard::drop` (kills anvil + removes the manifest) and kills any
    // in-flight `forge` child via `kill_on_drop`.
    // `Box::pin` keeps the large `run_e2e` body future off the stack
    // (clippy::large_futures fires above ~16 KB).
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run_e2e()))
        .await
        .with_context(|| format!("anvil-e2e exceeded the overall {OVERALL_TIMEOUT:?} timeout"))?
}

#[allow(
    clippy::cognitive_complexity,
    clippy::too_many_lines,
    reason = "single end-to-end settlement journey: each step depends on the previous step's chain \
              and runtime state, so decomposing it would thread a state bundle through helpers \
              without reducing the journey's length or making it easier to follow"
)]
async fn run_e2e() -> anyhow::Result<()> {
    // Surface the redeemer/watcher background-task logs (the `warn!` carrying an
    // on-chain revert reason is the key diagnostic when a `redeem` poll times
    // out). Those tasks run on tokio worker threads under `multi_thread`, so write
    // to process stderr — NOT `with_test_writer`, whose libtest thread-local
    // capture is set only on the test's main thread and would drop cross-thread
    // output. nextest captures the process's stderr and shows it on failure.
    // Layered with `CapCountLayer` (batched-read registration proof, see the
    // `BATCHED-READ REGISTRATION PROOF` section below) so the redeemer's own
    // `cap_count` per landed redeem is captured for assertions, not just
    // printed.
    // `try_init` is idempotent (harmless on a re-run).
    let cap_count_log: Arc<Mutex<HashMap<B256, usize>>> = Arc::new(Mutex::new(HashMap::new()));
    // `tracing_subscriber::fmt()` (the builder this replaces) defaults its
    // filter to `INFO`; a bare `Registry` + layers has no such default and
    // would pass every level (including the very chatty `TRACE` RPC/transport
    // spans), so each layer restates the `INFO` floor explicitly via
    // `EnvFilter` (honoring `RUST_LOG` if set, `info` otherwise).
    let level = || {
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
    };
    let _ = tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(std::io::stderr)
                .with_filter(level()),
        )
        .with(
            CapCountLayer {
                captured: Arc::clone(&cap_count_log),
            }
            .with_filter(level()),
        )
        .try_init();

    let contracts = contracts_dir();

    // ---- 0. Build contracts so artifacts + the deploy script are available.
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
    let port = free_port();
    let rpc_url = format!("http://127.0.0.1:{port}");
    let child = Command::new("anvil")
        .args([
            "--port",
            &port.to_string(),
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

    // ---- 2. Identities. The node holds two keys: an iroh ed25519 key (its
    // NodeId + the registration ownership proof) and an eth key (staking,
    // binding signature, and the redemption-tx signer). The `client` account is
    // the seller-path buyer that opens a pool and pays the node.
    let node_iroh_sk = fresh_key();
    let node_id = B256::from(*node_iroh_sk.public().as_bytes());
    let node_pub = node_iroh_sk.public();
    let node_signer = PrivateKeySigner::random();
    let node_addr = node_signer.address();
    let client_signer = Arc::new(PrivateKeySigner::random());
    let client_addr = client_signer.address();

    // Fund the node + client EOAs with gas (deployer is anvil-funded).
    for who in [node_addr, client_addr] {
        let _: serde_json::Value = admin
            .raw_request(
                "anvil_setBalance".into(),
                (
                    who,
                    U256::from(100u64) * U256::from(10u64).pow(U256::from(18)),
                ),
            )
            .await?;
    }

    // ---- 3. Deploy mock USDC (mintable) from its compiled bytecode, then run
    // the production deploy script with USDC_ADDRESS pointed at it.
    let usdc_addr = deploy_mock_usdc(&admin, &contracts).await?;
    run_deploy_script(&contracts, &rpc_url, usdc_addr, node_addr).await?;
    let (capacity_bond, payment_pool, fee_router, token, slash_judge) = read_manifest(&manifest)?;

    // Build the per-role providers + contract handles.
    // Simple nonce management mirrors production (`runtime::mod`, #904): a send
    // whose gas-estimate reverts must not leak a cached nonce and gap the lane.
    let node_provider = ProviderBuilder::new()
        .with_simple_nonce_management()
        .wallet(EthereumWallet::from(node_signer.clone()))
        .connect_http(url.clone());
    let client_provider = ProviderBuilder::new()
        .with_simple_nonce_management()
        .wallet(EthereumWallet::from((*client_signer).clone()))
        .connect_http(url.clone());

    let bond = CapacityBondWrite::new(capacity_bond, node_provider.clone());
    let token_erc20 = Erc20::new(token, node_provider.clone());
    let usdc_client = Erc20::new(usdc_addr, client_provider.clone());
    let usdc_admin = Erc20::new(usdc_addr, admin.clone());
    let pool_client = PaymentPool::new(payment_pool, client_provider.clone());
    let pool_read = PaymentPool::new(payment_pool, node_provider.clone());
    let fee_view = FeeRouterView::new(fee_router, node_provider.clone());

    // ---- 4. Activate the node operator: bond + registerNode → isActive.
    let min_bond: U256 = "50000000000000000000000".parse()?; // 50_000e18 (deploy default)
    token_erc20
        .approve(capacity_bond, min_bond)
        .send()
        .await?
        .get_receipt()
        .await?;
    bond.bond(min_bond).send().await?.get_receipt().await?;

    // Binding signature: eth key over EIP-712
    // RegisterNode(nodeId, nonce=0, termsHash) (ADR 019 § Terms Acceptance).
    // `termsHash` is read from the deployed contract so the signature matches
    // whatever genesis terms the deploy script committed.
    let terms_hash = bond.currentTermsHash().call().await?;
    let bind_domain = bind_node_id_domain(CHAIN_ID, capacity_bond);
    let binding_sig = node_signer
        .sign_hash_sync(&register_node_signing_hash(
            node_id,
            0,
            terms_hash,
            &bind_domain,
        ))?
        .as_bytes()
        .to_vec();
    // Ed25519 ownership proof: iroh key over
    // keccak256(nodeId ‖ ethAddr ‖ chainid ‖ registrationNonce=0) — PureEdDSA,
    // verified by the production Ed25519Verifier (dalek verify_strict parity).
    let mut msg = Vec::with_capacity(92);
    msg.extend_from_slice(node_id.as_slice()); // bytes32
    msg.extend_from_slice(node_addr.as_slice()); // address (20)
    msg.extend_from_slice(&U256::from(CHAIN_ID).to_be_bytes::<32>()); // uint256
    msg.extend_from_slice(&0u64.to_be_bytes()); // uint64 nonce
    let ed_sig = node_iroh_sk
        .sign(keccak256(&msg).as_slice())
        .to_bytes()
        .to_vec();

    bond.registerNode(
        node_id,
        Bytes::from_static(b"/ip4/127.0.0.1/udp/4242/quic-v1"),
        "us-east-1".to_string(),
        terms_hash,
        Bytes::from(binding_sig),
        Bytes::from(ed_sig),
    )
    .send()
    .await?
    .get_receipt()
    .await?;
    anyhow::ensure!(
        bond.isActive(node_addr).call().await?,
        "node operator must be active after stake + registerNode"
    );

    // ---- 5. Bring up the seller settlement service in-process.
    let payload = vec![0xABu8; 3 * MIB / 2]; // 1.5 MiB
    let (cache, hash, _cache_tmp) = cache_with_blob(&payload).await?;
    let store_tmp = tempfile::tempdir()?;
    // `PersistentPoolStateStore::open` enforces a `0o700` data_dir
    // (`identity::ensure_data_dir`); a umask of 002 leaves the tempdir at
    // 0o775, so tighten it explicitly (mirrors `key_gen_e2e.rs`).
    #[cfg(unix)]
    std::fs::set_permissions(
        store_tmp.path(),
        std::os::unix::fs::PermissionsExt::from_mode(0o700),
    )?;
    // One concrete redb-backed store backs the lane-state trait (the handler +
    // #527 replay guard), the capability sink/source (first-redemption
    // registration), and the watcher checkpoint — mirrors the runtime wiring.
    let concrete_store = Arc::new(PersistentPoolStateStore::open(store_tmp.path())?);
    let store: Arc<dyn PoolStateStore> = concrete_store.clone();
    let checkpoint_store: Arc<dyn KeyedCheckpointStore> = concrete_store.clone();

    let node_eth = Arc::new(node_signer.clone());
    let metrics = Arc::new(Metrics::new());
    let voucher_dom = voucher_domain(CHAIN_ID, payment_pool);
    let domains = HandlerDomains {
        slash: slash_judge_domain(CHAIN_ID, slash_judge),
        voucher: voucher_dom.clone(),
        binding: bind_domain.clone(),
    };
    // Redeem-hint channel, created by the caller (the settlement service no longer
    // mints it): the sender is wired into the handler at construction, the
    // receiver drives the service's redeemer loop.
    let (redeem_tx, redeem_rx) =
        tokio::sync::mpsc::channel(decdn_node::payment_settlement::REDEEM_HINT_CAPACITY);
    // Event-fed pool view, shared with the settlement service below (its watcher
    // is the writer). The capability-intake gate reads the on-chain pool owner
    // from it; without an owner the capability is dropped and no lane registers,
    // so the test waits for the projection to catch `PoolOpened` before serving.
    let pool_view = decdn_node::pool_view::PoolProjection::new();
    let handler = {
        let hint = redeem_tx.clone();
        let cap_store = Arc::clone(&concrete_store);
        let pool_view = pool_view.clone();
        build_handler_full_configured(
            node_pub,
            &node_eth,
            &metrics,
            permissive_limiter(&metrics),
            cache,
            Arc::clone(&store),
            RATE_PER_MB,
            &domains,
            0,
            16,
            move |deps| {
                deps.redeem_hint = Some(hint);
                // Owner-signed capability intake: the serve gate persists a
                // presented capability so the redeemer registers the signer on
                // its first redemption.
                deps.capability_sink =
                    Some(cap_store as Arc<dyn decdn_node::channel_store::CapabilitySink>);
                deps.pool_view =
                    Some(Arc::new(pool_view) as Arc<dyn decdn_node::pool_view::PoolView>);
            },
        )?
    };

    let capability_source: Arc<dyn decdn_node::payment_settlement::CapabilitySource> =
        Arc::new(StoredCapabilitySource::new(Arc::clone(&concrete_store)));
    let (service, settlement_route) = PoolSettlementService::bootstrap(
        node_provider.clone(),
        payment_pool,
        node_addr,
        Arc::clone(&store),
        Arc::clone(&checkpoint_store),
        Arc::clone(&handler),
        capability_source,
        U256::from(REDEEM_THRESHOLD_MICRO_USDC),
        300,
        Duration::from_secs(300),
        Arc::clone(&metrics),
        pool_view.clone(),
        redeem_tx,
        redeem_rx,
    )
    .await?;
    // The paid-watermark watcher is now a route on the shared multiplexed poller,
    // not a service-owned task. Spawn a single-route poller so the settlement
    // watcher actually runs (folds `PoolOpened` / `PoolRedeemed` events, drives
    // the pool projection) as it does in the runtime. The handle is held for the
    // test's duration; dropping it aborts the poller.
    let settlement_poller = {
        use decdn_node::chain_events::multiplexed_poller::{MultiplexedPollerBuilder, spawn};
        let poller =
            MultiplexedPollerBuilder::new(e2e_head(&node_provider), Duration::from_millis(250))
                .route(settlement_route)
                .build()?;
        spawn(node_provider.clone(), poller)
    };

    let (server_ep, server_addr) = local_endpoint(node_iroh_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), Arc::clone(&handler));
    let client_sk = fresh_key();
    let client_node_id = B256::from(*client_sk.public().as_bytes());
    let (client_ep, _) = local_endpoint(client_sk, vec![]).await?;
    let target = EndpointAddr::new(node_pub).with_ip_addr(server_addr);

    // Client funds + approves USDC for the pool contract.
    usdc_admin
        .mint(client_addr, U256::from(1_000_000_000u64))
        .send()
        .await?
        .get_receipt()
        .await?;
    usdc_client
        .approve(
            payment_pool,
            U256::from(DEPOSIT_MICRO_USDC) * U256::from(4u64),
        )
        .send()
        .await?
        .get_receipt()
        .await?;

    // ============================================================
    // SELLER PATH — the `client` account opens a pool, delivers one paid stream
    // carrying its self-capability (producing a genuine voucher), and the
    // redeemer redeems it. Assert the on-chain watermark advances and the byte
    // delta is routed into the operator's FeeRouter epoch counter.
    // ============================================================
    // `openPool` takes the pool's own `uint64` USDC width.
    let deposit = DEPOSIT_MICRO_USDC;
    let open_receipt = pool_client
        .openPool(deposit)
        .send()
        .await?
        .get_receipt()
        .await?;
    let pool_id = open_receipt
        .inner
        .logs()
        .iter()
        .filter_map(|l| l.log_decode::<PaymentPool::PoolOpened>().ok())
        .find(|d| d.inner.data.owner == client_addr)
        .map(|d| d.inner.data.poolId)
        .ok_or_else(|| anyhow::anyhow!("PoolOpened event missing from openPool receipt"))?;

    // Floor for the `PoolRedeemed` gas-delta scan below (#eth-calls-efficiency
    // batched-read proof): no redeem on this lane can land before this block.
    let redeem_scan_from = node_provider.get_block_number().await?;

    // Wait for the settlement watcher to fold `PoolOpened` into the shared pool
    // projection: the capability-intake gate reads the owner from it, and drops
    // the capability if the owner is not yet known. In production a serve request
    // against a just-opened pool would fail open (serve, defer registration); the
    // test pins the owner first so the single lane deterministically registers.
    {
        use decdn_node::pool_view::PoolView;
        let view = pool_view.clone();
        let owner = poll_until(Duration::from_secs(30), || {
            let view = view.clone();
            async move { view.status(pool_id).await.map(|s| s.owner) }
        })
        .await;
        anyhow::ensure!(
            owner == Some(client_addr),
            "pool projection did not observe PoolOpened owner in time"
        );
    }

    // Present the pool owner's self-issued capability (single-user: the owner
    // delegates spend to its own key) plus the ADR 005 ownership binding. The
    // handler verifies the capability against the on-chain pool owner (`pool_view`)
    // and registers the `(pool_id, client, node)` lane so it accepts vouchers.
    // Uncapped, matching production self-issue: the delegate IS the pool
    // owner, so the pool deposit — not the capability cap — is the real
    // spending bound.
    let capability = issue_self_capability(
        client_signer.as_ref(),
        pool_id,
        SELF_CAPABILITY_CAP,
        unix_now() + 1_000_000,
        &voucher_dom,
    )?;
    let binding = sign_client_binding(client_signer.as_ref(), client_node_id, &bind_domain)?;
    let ctx = PoolContext {
        pool_id,
        provider: node_addr,
        deposit: U256::from(deposit),
        client_signer: Arc::clone(&client_signer),
        voucher_domain: voucher_dom.clone(),
        prior_bytes_delivered: U256::ZERO,
        prior_amount: U256::ZERO,
        client_binding: Some(binding),
        capability: Some(capability),
    };
    let got = stream_fetch(
        &client_ep,
        target.clone(),
        &ctx,
        &domains.slash,
        node_addr,
        *hash.as_bytes(),
        0,
        0x00c0_ffe1,
        Duration::from_secs(30),
    )
    .await?;
    anyhow::ensure!(
        got.as_ref() == payload.as_slice(),
        "seller delivery mismatch"
    );

    // The redeemer normalizes the voucher `v`-byte and submits `redeem`; the
    // contract's ECDSA.recover accepts it and the lane's on-chain watermark
    // advances (`getWatermark(...).bytesDelivered` becomes non-zero).
    let watermark = poll_until(Duration::from_secs(60), || {
        let pool = pool_read.clone();
        async move {
            pool.getWatermark(pool_id, client_addr, node_addr)
                .call()
                .await
                .ok()
                .filter(|lane| lane.bytesDelivered > 0)
        }
    })
    .await;
    anyhow::ensure!(
        watermark.is_some(),
        "on-chain redeem never landed (normalized v-byte not accepted, or capability lane not registered)"
    );

    // FeeRouter routing: the redeem routed its byte delta into the operator's
    // per-epoch counter. Which voucher the single redeem lands on is
    // timing-dependent, so assert it incremented, not an exact value.
    let epoch = current_epoch(&node_provider).await?;
    let routed = fee_view.bytesPerEpoch(node_addr, epoch).call().await?;
    anyhow::ensure!(
        routed > U256::ZERO,
        "FeeRouter.bytesPerEpoch did not increment (settlement not routed): {routed}"
    );

    // ------------------------------------------------------------
    // BATCHED-READ REGISTRATION PROOF — the redeemer's first redeem on this
    // lane must have attached a `CapabilityReg` (the signer registers
    // on-chain) and persisted the observed expiry to `registered_until`; a
    // second sweep on the identical lane must then skip the `getAuthorizations`
    // read entirely (registration is already known) while still advancing the
    // paid watermark.
    // ------------------------------------------------------------
    let auth_after_first = pool_read
        .getAuthorization(pool_id, client_addr)
        .call()
        .await?;
    anyhow::ensure!(
        auth_after_first.cap != 0,
        "first redeem must land an on-chain Authorization for the signer (cap == 0)"
    );
    let lane_key = decdn_incentive::LaneKey {
        pool_id,
        signer: client_addr,
        provider: node_addr,
    };
    // The redeem's on-chain confirmation (`getWatermark` above) is observable
    // via `eth_call` immediately after the block is mined, but the redeemer's
    // `store.set_registered_until` (crates/node/src/payment_settlement.rs:1232)
    // runs in the same task after `TxOutcome::Landed`. Poll the store so the
    // assertion does not race the buffered write — an immediate check flaked
    // under load (run 32915992488).
    let lane_after_first = poll_until(Duration::from_secs(10), || {
        let store = Arc::clone(&store);
        async move {
            let st = store.get(lane_key).ok().flatten()?;
            (st.registered_until != 0).then_some(st)
        }
    })
    .await
    .ok_or_else(|| {
        let snapshot = store.get(lane_key).ok().flatten();
        anyhow::anyhow!(
            "registered_until must be persisted once the first redeem's CapabilityReg lands; \
             got lane={snapshot:?} auth_cap={} auth_expiry={}",
            auth_after_first.cap,
            auth_after_first.expiry
        )
    })?;
    // Ensure the persisted `registered_until` is visible to the next
    // `plan_lanes` before the second delivery. A fixed 100ms sleep previously
    // masked the race where `store.get` already showed the value but the next
    // `load_all` snapshot still raced; polling is deterministic.
    let _ = poll_until(Duration::from_secs(5), || {
        let store = Arc::clone(&store);
        async move {
            let st = store.get(lane_key).ok().flatten()?;
            (st.registered_until != 0).then_some(())
        }
    })
    .await;
    let watermark_after_first = watermark
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("watermark checked above"))?
        .clone();

    // Re-deliver the same blob on the identical lane, continuing the ledger from
    // the first delivery's cumulative totals as the NODE holds them, not as the
    // chain shows them. The two differ: a delivery settles over several
    // redemptions now — the chain meter advances a chunk at a time and the closing
    // sealed voucher folds the tail — so the on-chain watermark above is whatever
    // the first redeem to land happened to carry, a prefix of the delivery. A
    // payer that resumed from a prefix would open its next chain behind the lane's
    // own watermark, and the node would refuse to anchor it. A real buyer never
    // has this problem: it resumes from its OWN ledger, which is what
    // `LaneState::owed` mirrors here.
    //
    // No `capability` is attached this time — the lane already registered on its
    // first delivery, so a second sweep must resolve the redemption purely from
    // the persisted `registered_until` watermark rather than a fresh capability
    // grant.
    let second_ctx = PoolContext {
        prior_bytes_delivered: lane_after_first.owed_bytes(),
        prior_amount: lane_after_first.owed(),
        capability: None,
        ..ctx.clone()
    };
    let got_again = stream_fetch(
        &client_ep,
        target.clone(),
        &second_ctx,
        &domains.slash,
        node_addr,
        *hash.as_bytes(),
        0,
        0x00c0_ffe2,
        Duration::from_secs(30),
    )
    .await?;
    anyhow::ensure!(
        got_again.as_ref() == payload.as_slice(),
        "second seller delivery mismatch"
    );

    // The second sweep's `redeem` must still land: the paid watermark advances
    // past the first redeem's value even though no fresh `CapabilityReg` was
    // needed.
    let watermark_2 = poll_until(Duration::from_secs(60), || {
        let pool = pool_read.clone();
        let floor = watermark_after_first.bytesDelivered;
        async move {
            pool.getWatermark(pool_id, client_addr, node_addr)
                .call()
                .await
                .ok()
                .filter(|lane| lane.bytesDelivered > floor)
        }
    })
    .await;
    anyhow::ensure!(
        watermark_2.is_some(),
        "second redeem on an already-registered lane never landed"
    );

    // The Authorization is unchanged by the second sweep (registration is
    // set-once on-chain; a re-attached `CapabilityReg` would be a harmless
    // no-op — the `cap_count` assertion just below is what actually proves
    // this lane's redeemer never built one).
    let auth_after_second = pool_read
        .getAuthorization(pool_id, client_addr)
        .call()
        .await?;
    anyhow::ensure!(
        auth_after_second.cap == auth_after_first.cap
            && auth_after_second.expiry == auth_after_first.expiry,
        "Authorization must not change across the second, already-registered sweep"
    );
    // Poll for the same reason as `lane_after_first`: the second redeem's
    // `store.set_registered_until` (if any) and the watcher `paid` update race
    // the `getWatermark` poll above.
    let _lane_after_second = poll_until(Duration::from_secs(10), || {
        let store = Arc::clone(&store);
        async move {
            let st = store.get(lane_key).ok().flatten()?;
            (st.registered_until != 0).then_some(st)
        }
    })
    .await
    .ok_or_else(|| {
        let snapshot = store.get(lane_key).ok().flatten();
        anyhow::anyhow!(
            "registered_until must remain persisted after the second sweep; got lane={snapshot:?}"
        )
    })?;

    // THE REGRESSION-CATCHING ASSERTION: locate the two on-chain transactions
    // that landed the two redeems, and read back the redeemer's own
    // `cap_count` for each (captured by `CapCountLayer`, see `run_e2e`'s
    // subscriber setup) — the direct, deterministic proof of how many
    // `CapabilityReg`s each sweep attached. `_registerCapability`
    // (contracts/src/PaymentPool.sol:698-699) is a silent, event-less,
    // idempotent no-op on a re-attach — it returns before its SSTORE or
    // signature check once `cap != 0` — so neither an on-chain event nor a
    // gas-cost delta reliably distinguishes "attached a CapabilityReg that
    // no-op'd" from "never attached one" (the calldata-only remainder is a few
    // hundred gas, swamped by ordinary per-run variance). Reading the
    // redeemer's own `cap_count` field is what actually asserts on the code's
    // decision instead of an unreliable on-chain proxy for it. Both lookups
    // happen only now, after both deliveries have already landed, so neither
    // adds RPC latency between the first redeem confirming and the second
    // delivery starting.
    let (tx_first, block_first) = redeem_tx_for_lane(
        &node_provider,
        payment_pool,
        pool_id,
        node_addr,
        redeem_scan_from,
    )
    .await?;
    // `CapCountLayer` is fed by `submit_chunk`'s `info!(cap_count)` which runs
    // after `store.set_registered_until` in the same task, so poll for its
    // capture to avoid racing the log layer's `on_event` delivery.
    let cap_count_first = poll_until(Duration::from_secs(10), || {
        let captured = Arc::clone(&cap_count_log);
        let tx = tx_first;
        async move { cap_count_for_tx(&captured, tx).ok() }
    })
    .await
    .ok_or_else(|| anyhow::anyhow!("no captured cap_count log line for redeem tx {tx_first}"))?;
    anyhow::ensure!(
        cap_count_first == 1,
        "first redeem must attach exactly one CapabilityReg (the signer registers on-chain \
         for the first time): cap_count={cap_count_first}"
    );
    let (tx_second, _block_second) = redeem_tx_for_lane(
        &node_provider,
        payment_pool,
        pool_id,
        node_addr,
        block_first + 1,
    )
    .await?;
    let cap_count_second = poll_until(Duration::from_secs(10), || {
        let captured = Arc::clone(&cap_count_log);
        let tx = tx_second;
        async move { cap_count_for_tx(&captured, tx).ok() }
    })
    .await
    .ok_or_else(|| anyhow::anyhow!("no captured cap_count log line for redeem tx {tx_second}"))?;
    anyhow::ensure!(
        cap_count_second == 0,
        "second sweep must attach NO CapabilityReg — registration is already known from the \
         persisted registered_until watermark, so the batched-read skip must fire: \
         cap_count={cap_count_second}"
    );

    // ============================================================
    // BUYER PATH — a dedicated `buyer_addr` identity drives a `BuyerPoolService`:
    // open a pool → deliver over the service-produced PoolContext → top up →
    // close + reclaim after the grace window. Asserts the full deposit refunds
    // and the local record is dropped.
    // ============================================================
    let buyer_signer = Arc::new(PrivateKeySigner::random());
    let buyer_addr = buyer_signer.address();
    let _: serde_json::Value = admin
        .raw_request(
            "anvil_setBalance".into(),
            (
                buyer_addr,
                U256::from(100u64) * U256::from(10u64).pow(U256::from(18)),
            ),
        )
        .await?;
    // Simple nonce management mirrors the buyer wallet provider in production
    // (#904).
    let buyer_provider = ProviderBuilder::new()
        .with_simple_nonce_management()
        .wallet(EthereumWallet::from((*buyer_signer).clone()))
        .connect_http(url.clone());
    let usdc_buyer = Erc20::new(usdc_addr, buyer_provider.clone());
    let buyer_pool = PaymentPool::new(payment_pool, buyer_provider.clone());
    // Fund the buyer: one pool open + a top-up.
    usdc_admin
        .mint(
            buyer_addr,
            U256::from(DEPOSIT_MICRO_USDC) * U256::from(4u64),
        )
        .send()
        .await?
        .get_receipt()
        .await?;

    let buyer_store = Arc::new(MemoryBuyerPoolStore::new());
    let buyer_store_dyn: Arc<dyn BuyerPoolStore> = buyer_store.clone();
    let buyer_service = BuyerPoolService::bootstrap(
        buyer_provider.clone(),
        payment_pool,
        buyer_addr,
        buyer_store_dyn,
        Arc::clone(&buyer_signer),
        voucher_dom.clone(),
        U256::from(DEPOSIT_MICRO_USDC), // working_deposit
        true, // fresh buyer identity → issue the one-time max USDC approval
        Arc::new(Metrics::new()),
    )
    .await?;

    // Lazy open against the provider → a fresh on-chain pool + a persisted record.
    // The returned context is pinned to the provider and already carries the
    // buyer's self-issued capability; attach an ownership binding so the serve
    // gate registers the lane.
    let buyer_ctx = buyer_service
        .open_or_reuse_pool(node_addr, OPEN_BUDGET)
        .await?;
    let buyer_pool_id = buyer_ctx.pool_id;
    let on_chain = buyer_pool.getPool(buyer_pool_id).call().await?;
    anyhow::ensure!(
        on_chain.owner == buyer_addr,
        "buyer pool opened with the wrong owner"
    );
    anyhow::ensure!(
        buyer_store.len() == 1,
        "buyer service should track exactly one pool after open"
    );

    // Deliver the 0.5 MiB suffix over the service-produced context (proving the
    // buyer open → sign path end-to-end). The binding proves ownership of the
    // requester endpoint (`client_ep` here) to the serve gate.
    let buyer_binding = sign_client_binding(buyer_signer.as_ref(), client_node_id, &bind_domain)?;
    let buyer_ctx = buyer_ctx.with_client_binding(buyer_binding);
    // As with the seller pool: wait for the settlement watcher to fold this
    // freshly-opened pool's `PoolOpened` into the shared projection, so the
    // capability-intake gate can confirm the owner and register the lane.
    {
        use decdn_node::pool_view::PoolView;
        let view = pool_view.clone();
        let owner = poll_until(Duration::from_secs(30), || {
            let view = view.clone();
            async move { view.status(buyer_pool_id).await.map(|s| s.owner) }
        })
        .await;
        anyhow::ensure!(
            owner == Some(buyer_addr),
            "pool projection did not observe the buyer pool's PoolOpened owner in time"
        );
    }
    let suffix = stream_fetch(
        &client_ep,
        target.clone(),
        &buyer_ctx,
        &domains.slash,
        node_addr,
        *hash.as_bytes(),
        MIB as u64,
        0x00c0_ffe3,
        Duration::from_secs(30),
    )
    .await?;
    anyhow::ensure!(
        suffix.as_ref() == &payload[MIB..],
        "buyer suffix delivery mismatch"
    );

    // Top-up: raise the pool toward the working deposit + a margin and assert the
    // on-chain deposit reflects it.
    let target_deposit = U256::from(DEPOSIT_MICRO_USDC) + U256::from(TOPUP_MICRO_USDC);
    buyer_service.top_up_pool(target_deposit).await?;
    anyhow::ensure!(
        U256::from(buyer_pool.getPool(buyer_pool_id).call().await?.deposit) >= target_deposit,
        "topUp must raise the on-chain deposit"
    );

    // Reclaim: the buyer closes its own pool, the chain is warped past the grace
    // window, and one reclaim pass refunds the residual and drops the record.
    buyer_pool
        .closePool(buyer_pool_id)
        .send()
        .await?
        .get_receipt()
        .await?;
    let balance_before = usdc_buyer.balanceOf(buyer_addr).call().await?;
    let _: serde_json::Value = node_provider
        .raw_request("evm_increaseTime".into(), (GRACE_WINDOW_WARP_SECS,))
        .await?;
    let _: serde_json::Value = node_provider.raw_request("evm_mine".into(), ()).await?;

    buyer_service.sweep_reclaimable_once().await;

    anyhow::ensure!(
        buyer_store.get_by_owner(buyer_addr)?.is_none(),
        "reclaimed buyer pool record must be dropped"
    );
    let balance_after = usdc_buyer.balanceOf(buyer_addr).call().await?;
    anyhow::ensure!(
        balance_after > balance_before,
        "reclaim must refund the residual deposit; got {balance_before} → {balance_after}"
    );
    let reclaimed = buyer_pool.getPool(buyer_pool_id).call().await?;
    anyhow::ensure!(
        matches!(reclaimed.status, PaymentPool::Status::Closed),
        "reclaimed pool must be Closed on-chain"
    );

    // Retire the seller service: stop the shared poller (flushes the settlement
    // route's scan checkpoint), then the service's graceful-shutdown path
    // (quiesce the redeemer, run a final redeem sweep).
    settlement_poller.shutdown();
    service.shutdown(Duration::from_secs(30)).await;

    client_ep.close().await;
    server_ep.close().await;
    let _ = server_task.await;
    Ok(())
}

/// Find the `PoolRedeemed` event for `(pool_id, provider)` at or after
/// `from_block`, and return `(tx_hash, block_number)` for the redeem that
/// emitted it. `poll_until` absorbs the (normally instant, on anvil) gap
/// between the tx landing and the log being queryable.
async fn redeem_tx_for_lane<P>(
    provider: &P,
    payment_pool: Address,
    pool_id: B256,
    provider_addr: Address,
    from_block: u64,
) -> anyhow::Result<(B256, u64)>
where
    P: Provider + Clone,
{
    poll_until(Duration::from_secs(30), || {
        let provider = provider.clone();
        async move {
            let filter = Filter::new()
                .address(payment_pool)
                .event_signature(PaymentPool::PoolRedeemed::SIGNATURE_HASH)
                .from_block(from_block);
            let logs = provider.get_logs(&filter).await.ok()?;
            let hit = logs
                .iter()
                .filter_map(|l| l.log_decode::<PaymentPool::PoolRedeemed>().ok())
                .find(|d| {
                    d.inner.data.poolId == pool_id && d.inner.data.provider == provider_addr
                })?;
            Some((hit.transaction_hash?, hit.block_number?))
        }
    })
    .await
    .ok_or_else(|| {
        anyhow::anyhow!("no PoolRedeemed event for the lane found from block {from_block}")
    })
}

/// Read back the redeemer's own `cap_count` — how many `CapabilityReg`s it
/// attached — for `tx_hash`, as captured by [`CapCountLayer`] from the
/// `"batched lane redemption landed (redeemMany)"` info log
/// (`crates/node/src/payment_settlement.rs`). Errors rather than defaulting to
/// `0` when the tx was never observed: a missing capture is a plumbing bug in
/// this test, not evidence of "no `CapabilityReg` attached", and must not be
/// silently conflated with a real negative result.
fn cap_count_for_tx(
    captured: &Mutex<HashMap<B256, usize>>,
    tx_hash: B256,
) -> anyhow::Result<usize> {
    captured
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&tx_hash)
        .copied()
        .ok_or_else(|| anyhow::anyhow!("no captured cap_count log line for redeem tx {tx_hash}"))
}

/// Current `FeeRouter` epoch = `block.timestamp / epochLength`.
async fn current_epoch<P: Provider>(provider: &P) -> anyhow::Result<u64> {
    let block: serde_json::Value = provider
        .raw_request("eth_getBlockByNumber".into(), ("latest", false))
        .await?;
    let ts_hex = block["timestamp"].as_str().expect("block timestamp");
    let ts = u64::from_str_radix(ts_hex.trim_start_matches("0x"), 16)?;
    Ok(ts / EPOCH_LENGTH_SECS)
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
    let code: Bytes = code_hex.parse()?;
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
/// from the anvil dev deployer. `INITIAL_TOKEN_HOLDER` is the node so it holds
/// the staking TOKEN directly.
///
/// `forge script --broadcast` fails transiently under runner CPU contention in
/// two ways, both retried up to `DEPLOY_ATTEMPTS` times: a *stall* in receipt-wait
/// (timeout, issue #785) and a *broadcast-phase non-zero exit* (#883) where the
/// script body completed (`Script ran successfully` printed) but tx submission hit
/// a nonce/RPC/anvil hiccup. Retry is safe: each run broadcasts from a fresh
/// deployer nonce (new contract addresses) and `FORCE_OVERWRITE_MANIFEST` rewrites
/// the manifest the test reads, so a completed retry fully supersedes a killed or
/// half-broadcast one. A non-zero exit *before* the body completes (a genuine
/// revert or script bug — no success marker) is deterministic and fails fast with
/// the full output rather than retrying a guaranteed-identical failure. See
/// `forge_script_body_completed` for the classification.
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
            // DeployProtocol.s.sol requires a genesis VETTER_ROLE holder (a deploy
            // with none can vet no publisher and is a governance deadlock). This
            // suite exercises settlement, not vetting; the deployer plays the role.
            .env("INITIAL_VETTER", DEPLOYER_ADDR)
            // ADR 019 § Terms Acceptance — DeployProtocol.s.sol requires a
            // non-zero genesis terms hash (CapacityBond rejects the zero
            // sentinel). The registration path reads it back from the contract.
            .env(
                "CURRENT_TERMS_HASH",
                "0x0000000000000000000000000000000000000000000000000000000000000001",
            )
            .env("FORCE_OVERWRITE_MANIFEST", "true");
        // A spawn failure (`forge` missing) is deterministic — `?` fails fast
        // rather than masquerading as a stall and burning a retry.
        match forge_output(cmd, DEPLOY_TIMEOUT, "forge script DeployProtocol").await? {
            Ok(out) if out.status.success() => return Ok(()),
            // Non-zero exit *after* the script body completed (#883) — the failure
            // was in the broadcast / tx-submission phase, a transient hiccup worth
            // retrying like a stall. A non-zero exit *before* the body completed is
            // a deterministic revert/script bug and fails fast with full output.
            Ok(out) if forge_script_body_completed(&out.stdout) => {
                if attempt == DEPLOY_ATTEMPTS {
                    anyhow::bail!(
                        "forge script DeployProtocol failed after {DEPLOY_ATTEMPTS} attempts; the final attempt completed the script body but exited non-zero during broadcast:\n{}\n{}",
                        String::from_utf8_lossy(&out.stdout),
                        String::from_utf8_lossy(&out.stderr)
                    );
                }
                tracing::warn!(
                    "forge script DeployProtocol attempt {attempt}/{DEPLOY_ATTEMPTS} completed the script body but exited non-zero during broadcast (transient nonce/RPC hiccup), retrying immediately"
                );
            }
            // Deterministic failure (revert / script bug) — surface it and stop.
            Ok(out) => {
                anyhow::bail!(
                    "forge script DeployProtocol exited non-zero before the script body completed (revert or script bug):\n{}\n{}",
                    String::from_utf8_lossy(&out.stdout),
                    String::from_utf8_lossy(&out.stderr)
                )
            }
            // Stall (#785) — retry while attempts remain, else surface it.
            Err(timeout) => {
                if attempt == DEPLOY_ATTEMPTS {
                    anyhow::bail!(
                        "forge script DeployProtocol failed after {DEPLOY_ATTEMPTS} attempts; the final attempt stalled (timed out after {timeout:?})"
                    );
                }
                tracing::warn!(
                    "forge script DeployProtocol attempt {attempt}/{DEPLOY_ATTEMPTS} stalled (killed after {timeout:?}), retrying immediately"
                );
            }
        }
    }
    // Reached only if DEPLOY_ATTEMPTS == 0; the loop returns on every other path.
    anyhow::bail!("DEPLOY_ATTEMPTS must be >= 1 (was {DEPLOY_ATTEMPTS})")
}

/// Read `(CapacityBond, PaymentPool, FeeRouter, Token, SlashJudge)` from the
/// deploy manifest.
fn read_manifest(path: &Path) -> anyhow::Result<(Address, Address, Address, Address, Address)> {
    let json: serde_json::Value = serde_json::from_slice(&std::fs::read(path)?)?;
    let c = &json["contracts"];
    let get = |k: &str| -> anyhow::Result<Address> {
        Ok(c[k]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("missing {k}"))?
            .parse()?)
    };
    Ok((
        get("CapacityBond")?,
        get("PaymentPool")?,
        get("FeeRouter")?,
        get("Token")?,
        get("SlashJudge")?,
    ))
}
