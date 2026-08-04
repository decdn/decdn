//! Live anvil-backed e2e for the on-chain `PaymentChannel` seller settlement
//! path (issue #745, on top of PR #743) and the buyer-side open/reuse/reclaim
//! path (#744). Gated behind the `anvil-e2e` feature so the default test run
//! stays fast and needs no `anvil`/`forge` binaries.
//!
//! PR #743 verified the seller settlement path statically (build, clippy, unit
//! tests, ABI cross-check) but never ran it against a chain. This test closes
//! the two runtime-only gaps it called out:
//!
//!   1. **On-chain acceptance of the normalized voucher signature `v`-byte** —
//!      a real client-signed voucher must be accepted by the contract's
//!      `ECDSA.recover` when the node submits `withdraw` / `closeChannel` /
//!      `disputeChannel`.
//!   2. **Live event decode** — the [`PaymentChannelService`] watcher's
//!      subscribe+decode path running against a live RPC. `ChannelOpened` and
//!      `ChannelSettled` decode are asserted directly (via the resulting store
//!      transitions — persist then forget). `ChannelCloseInitiated` drives the
//!      self-defense dispute reaction (#1586): the test closes channel 2 at a
//!      stale watermark and asserts the watcher reacts with `disputeChannel`,
//!      ratcheting the on-chain claim up to the node's persisted voucher.
//!
//! ## Shape
//!
//! 1. Spawn a local `anvil`, deploy a mintable mock USDC, then deploy the full
//!    protocol via the production `forge script DeployProtocol.s.sol` (reading
//!    the deployed addresses from the `deployments/<chainId>.json` manifest).
//! 2. Activate the node operator on-chain: `stake` + `registerNode` (the iroh
//!    ed25519 node key signs the ownership proof the production
//!    `Ed25519Verifier` checks; the eth key signs the EIP-712 binding).
//! 3. Bring up the seller settlement service in-process
//!    ([`PaymentChannelService::bootstrap`] + a [`PersistentChannelStateStore`]
//!    + a [`ClientHandler`]) pointed at the anvil RPC.
//! 4. Channel 1 — client `openChannel` on-chain → assert the watcher decodes
//!    `ChannelOpened` and persists the channel (closes the documented #327
//!    `WrongChannel` gap); run a real iroh paid-delivery roundtrip so a genuine
//!    voucher is produced; assert the node's threshold `withdraw` lands on-chain
//!    (`getChannel().withdrawnAmount` advances) and `FeeRouter.bytesPerEpoch`
//!    increments.
//! 5. Channel 2 — deliver a claim *below* the redeem threshold (stays
//!    un-redeemed); close it on-chain WITHOUT a voucher (a stale `claimedNonce
//!    == 0` watermark) while the settlement watcher stays alive → assert the
//!    watcher self-defense-disputes (#1586), ratcheting `claimedNonce`/
//!    `claimedAmount` up to the node's persisted voucher; then after the dispute
//!    window `settleChannel` → assert the watcher decodes `ChannelSettled` and
//!    `forget`s the row from the store. (The service's own graceful-shutdown
//!    close path — which now cancels the watcher — is exercised after the buyer
//!    path, before channel 3.)
//! 6. Buyer path (#744) — the `client` account drives a [`BuyerChannelService`]
//!    against the registered provider: `open_or_reuse_channel` opens a channel
//!    on-chain, a delivery signs vouchers via the service-produced
//!    [`ChannelContext`], a second `open_or_reuse_channel` reuses it (no new
//!    `openChannel`), and `sweep_expired_once` runs `reclaimExpired` after the
//!    chain is warped past expiry — asserting the full deposit refunds and the
//!    record is dropped.
//!
//! Requires `anvil` + `forge` on `PATH` (the CI job provisions Foundry). If
//! they are absent the test fails loudly rather than silently skipping — it is
//! opt-in via the feature, so reaching it means the caller asked for it.

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

use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::Duration;

use alloy::network::{EthereumWallet, TransactionBuilder};
use alloy::primitives::{Address, B256, Bytes, U256, keccak256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::SignerSync;
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use decdn_incentive::payment_channel::PaymentChannel;
use decdn_incentive::{
    BuyerChannelStore, ChannelStateStore, KeyedCheckpointStore, MemoryBuyerChannelStore,
    MemoryPendingSettleStore, PendingSettleStore, bind_node_id_domain, register_node_signing_hash,
    slash_judge_domain, voucher_domain,
};
use decdn_node::buyer_channel::{BuyerChannelService, ChannelOpenPending};
use decdn_node::chain_events::shared_head::{HeadSource, SharedHead};
use decdn_node::channel_store::{BuyerChannelStoreHandle, PersistentChannelStateStore};
use decdn_node::client_requester::{ChannelContext, stream_fetch};
use decdn_node::metrics::Metrics;
use decdn_node::payment_settlement::{AutoSettleConfig, PaymentChannelService};
use decdn_protocol::ALPN_CLIENT;
use iroh::EndpointAddr;

mod support;
use support::{
    HandlerDomains, build_handler_full_configured, cache_with_blob, fresh_key, local_endpoint,
    permissive_limiter, spawn_server,
};

// Test-only chain id in the `deployments/3133769*.json` gitignore range so the
// forge-script manifest never collides with (or is committed alongside) a real
// chain's manifest.
/// Caller budget for `open_or_reuse_channel` in these tests (#1143). Generous:
/// anvil mines instantly, and the budget bounds how long a CALLER waits, not how
/// long the open may take — a tight value here would make the tests flaky on a
/// loaded runner without testing anything the unit tests don't already cover.
const OPEN_BUDGET: Duration = Duration::from_secs(60);

const CHAIN_ID: u64 = 31_337_690;
// Default anvil dev account #0 (mnemonic "test test … junk") — funded with ETH
// at genesis, used to broadcast the deploy. Not the forge-default sender the
// deploy script rejects.
const DEPLOYER_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
const DEPLOYER_ADDR: &str = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266";
// Default anvil dev account #1 — the in-process "admin" EOA (deploys the mock
// USDC, mints, funds gas, settles). Deliberately NOT account #0: that EOA is
// the `forge script` broadcaster, whose on-chain nonce the script advances by
// ~25; sharing it would desync alloy's cached nonce ("nonce too low").
const ADMIN_KEY: &str = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d";

const EPOCH_LENGTH_SECS: u64 = 7 * 24 * 60 * 60; // FeeRouter/CapacityBond constant
const DISPUTE_WINDOW_SECS: u64 = 48 * 60 * 60; // BaseProtocolDeploy PAYMENT_DISPUTE_WINDOW
const RATE_PER_MB: u64 = 10; // µUSDC per MiB (within deliveryFloor/ceiling)
const MIB: usize = 1024 * 1024;

// Settlement-economics knobs sized so channel 1's claim crosses the redeem
// threshold (auto-`withdraw`) and channel 2's does not (left for the
// shutdown-close path). Amounts below are the cumulative voucher *claims* (the
// final amount the client signs), not necessarily what the single redeem
// withdraws — see the FeeRouter assertion below.
//  - channel 1: 1.5 MiB delivered → claim = ceil(1.5*10) = 15 µUSDC ≥ 10
//    (its 1 MiB interval voucher, 10 µUSDC, already crosses the threshold)
//  - channel 2: 0.5 MiB delivered → claim = ceil(0.5*10) =  5 µUSDC < 10
const REDEEM_THRESHOLD_MICRO_USDC: u64 = 10;
// Self-defense-dispute residual floor (#1586) sized well below channel 2's
// ~5 µUSDC stale-close residual, so the node's dispute reaction fires in this
// test's tiny-amount economics; a no-op at the other call sites (no stale close
// occurs there, so `should_dispute` returns `None` on the equal-nonce close).
const DISPUTE_MIN_RESIDUAL_MICRO_USDC: u64 = 1;
const DEPOSIT_MICRO_USDC: u64 = 10_000_000; // 10 USDC (ADR 003 recommended minimum)
const TOPUP_MICRO_USDC: u64 = 2_000_000; // 2 USDC added via topUp in the buyer path (#744)
// Warp past any governable channel lifetime (max 365 days) so `reclaimExpired`
// is permitted on-chain in the buyer-path reclaim assertion (#744).
const CHANNEL_EXPIRY_WARP_SECS: u64 = 366 * 24 * 60 * 60;

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
// `poll_until` budgets (~560s) + build + deploy so a slow-but-legitimate run
// still surfaces its specific poll diagnostic, while a truly *unbounded* await
// (iroh, `get_receipt`) fails fast. Stays under the 15-minute CI job cap.
const OVERALL_TIMEOUT: Duration = Duration::from_secs(780);

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

/// A real [`SharedHead`] over the anvil provider, matching the poll interval each
/// service is bootstrapped with (so the TTL is 125 ms here, not the 3.5 s a
/// production 7 s interval yields). Each service gets its own rather than sharing
/// one: `service2`/`service3` model a node restarting against the same store, and
/// a head cache surviving that restart would not.
fn e2e_head<P>(provider: &P) -> Arc<dyn HeadSource>
where
    P: Provider + Clone + 'static,
{
    Arc::new(SharedHead::new(
        provider.clone(),
        Duration::from_millis(250),
    ))
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
    let stdout = b"No files changed, compilation skipped\nScript ran successfully.\n\n== Return ==\nd: struct BaseProtocolDeploy.Deployment Deployment({ token: 0x959, paymentChannel: 0x4ed })\n\n## Setting up 1 EVM.";
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
// `decdn_incentive` bindings. The seller-path read surface (`getChannel`,
// `settleChannel`, the `Channel` struct, the `Status` enum) is reused from
// `decdn_incentive::payment_channel::PaymentChannel` (the same ABI the runtime
// decodes) rather than re-declared here, so the load-bearing `Channel` layout
// has a single source of truth. Only the buyer-side `openChannel` — which the
// seller-only production binding omits — is declared locally.
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
    contract PaymentChannelOpen {
        // `voucherSigner` pins the voucher-signing address; the contract
        // resolves the zero address to `msg.sender` (self-signing).
        function openChannel(address provider, uint256 deposit, address voucherSigner)
            external
            returns (bytes32 channelId);
    }

    #[sol(rpc)]
    contract FeeRouterView {
        function bytesPerEpoch(address operator, uint64 epoch) external view returns (uint256);
    }
}

/// `channelId = keccak256(abi.encodePacked(client, provider, channelNonce))`
/// (matches `PaymentChannel.openChannel`).
fn derive_channel_id(client: Address, provider: Address, channel_nonce: u64) -> B256 {
    let mut packed = Vec::with_capacity(72);
    packed.extend_from_slice(client.as_slice());
    packed.extend_from_slice(provider.as_slice());
    packed.extend_from_slice(&U256::from(channel_nonce).to_be_bytes::<32>());
    keccak256(&packed)
}

#[tokio::test(flavor = "multi_thread")]
async fn e2e_onchain_payment_channel_settlement() -> anyhow::Result<()> {
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
    // on-chain revert reason is the key diagnostic when a `withdraw`/`close`
    // poll times out). Those tasks run on tokio worker threads under
    // `multi_thread`, so write to process stderr — NOT `with_test_writer`, whose
    // libtest thread-local capture is set only on the test's main thread and
    // would drop cross-thread output. nextest captures the process's stderr and
    // shows it on failure. `try_init` is idempotent (harmless on a re-run).
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
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
    // binding signature, and the settlement-tx signer).
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
    let (capacity_bond, payment_channel, fee_router, token, slash_judge) =
        read_manifest(&manifest)?;

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
    let pc_client = PaymentChannelOpen::new(payment_channel, client_provider.clone());
    let pc_read = PaymentChannel::new(payment_channel, node_provider.clone());
    let pc_settle = PaymentChannel::new(payment_channel, admin.clone());
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
    // `PersistentChannelStateStore::open` enforces a `0o700` data_dir
    // (`identity::ensure_data_dir`); a umask of 002 leaves the tempdir at
    // 0o775, so tighten it explicitly (mirrors `key_gen_e2e.rs`).
    #[cfg(unix)]
    std::fs::set_permissions(
        store_tmp.path(),
        std::os::unix::fs::PermissionsExt::from_mode(0o700),
    )?;
    // One concrete redb-backed store backs both the voucher-state trait (the
    // handler + #527 replay guard) and the pending-settle trait (the on-chain
    // settlement sweep, PR #743 review) — mirrors the runtime wiring.
    let concrete_store = Arc::new(PersistentChannelStateStore::open(store_tmp.path())?);
    let store: Arc<dyn ChannelStateStore> = concrete_store.clone();
    let pending_store: Arc<dyn PendingSettleStore> = concrete_store.clone();
    // Keep `concrete_store` alive (don't move it) so the downtime-backfill phase
    // at the end can re-bootstrap a second service against the same store.
    let checkpoint_store: Arc<dyn KeyedCheckpointStore> = concrete_store.clone();

    let node_eth = Arc::new(node_signer.clone());
    let metrics = Arc::new(Metrics::new());
    let domains = HandlerDomains {
        slash: slash_judge_domain(CHAIN_ID, slash_judge),
        voucher: voucher_domain(CHAIN_ID, payment_channel),
        binding: bind_domain.clone(),
    };
    // Redeem-hint channel, now created by the caller (the settlement service no
    // longer mints it): the sender is wired into the handler at construction, the
    // receiver drives the service's redeemer loop (#1254).
    let (redeem_tx, redeem_rx) =
        tokio::sync::mpsc::channel(decdn_node::payment_settlement::REDEEM_HINT_CAPACITY);
    let handler = build_handler_full_configured(
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
        |deps| deps.redeem_hint = Some(redeem_tx.clone()),
    )?;

    let service = PaymentChannelService::bootstrap(
        node_provider.clone(),
        payment_channel,
        node_addr,
        Arc::clone(&store),
        pending_store,
        checkpoint_store,
        Arc::clone(&handler),
        U256::from(REDEEM_THRESHOLD_MICRO_USDC),
        Duration::from_secs(300),
        AutoSettleConfig::default(),
        U256::from(DISPUTE_MIN_RESIDUAL_MICRO_USDC),
        Duration::from_millis(250),
        e2e_head(&node_provider),
        Arc::clone(&metrics),
        redeem_tx,
        redeem_rx,
    )
    .await?;

    let (server_ep, server_addr) = local_endpoint(node_iroh_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), Arc::clone(&handler));
    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let target = EndpointAddr::new(node_pub).with_ip_addr(server_addr);

    // No watcher-readiness sleep needed (#762): the bring-up backfill captures
    // the head block at `bootstrap` and `get_logs`-replays ChannelOpened up to
    // the block the live filters install at, so a channel opened before the
    // filters are live is still registered. The `poll_until` on channel
    // persistence below is the deterministic wait.

    // Client funds + approves USDC once (covers both channels).
    usdc_admin
        .mint(client_addr, U256::from(1_000_000_000u64))
        .send()
        .await?
        .get_receipt()
        .await?;
    usdc_client
        .approve(
            payment_channel,
            U256::from(DEPOSIT_MICRO_USDC) * U256::from(4u64),
        )
        .send()
        .await?
        .get_receipt()
        .await?;

    // ============================================================
    // CHANNEL 1 — open, deliver full blob, assert on-chain withdraw.
    // ============================================================
    let deposit = U256::from(DEPOSIT_MICRO_USDC);
    pc_client
        // self-signing: the contract resolves zero to msg.sender
        .openChannel(node_addr, deposit, Address::ZERO)
        .send()
        .await?
        .get_receipt()
        .await?;
    let id1 = derive_channel_id(client_addr, node_addr, 0);

    // GAP 2 (+ #327 WrongChannel close): the watcher decodes ChannelOpened and
    // persists the channel, so the handler will now accept its vouchers.
    let persisted = poll_until(Duration::from_secs(60), || {
        let store = Arc::clone(&store);
        async move { store.get(id1).ok().flatten() }
    })
    .await
    .ok_or_else(|| {
        anyhow::anyhow!(
            "watcher did not decode ChannelOpened / persist channel 1 (live event-decode gap)"
        )
    })?;
    // Confirm the event's non-indexed fields (not just the id topic)
    // round-tripped through the watcher's decode + ChannelState construction:
    // `expiresAt` feeds the expiry-sweep close path; `client`/`deposit` are
    // otherwise unasserted here.
    anyhow::ensure!(
        persisted.expires_at != 0,
        "persisted channel 1 has no expiry — ChannelOpened.expiresAt was not decoded"
    );
    anyhow::ensure!(
        persisted.client == client_addr,
        "persisted channel 1 client mismatch: {} != {client_addr}",
        persisted.client
    );
    anyhow::ensure!(
        persisted.deposit == deposit,
        "persisted channel 1 deposit mismatch: {} != {deposit}",
        persisted.deposit
    );

    // Real paid-delivery roundtrip → genuine client-signed voucher.
    let ctx1 = ChannelContext {
        channel_id: id1,
        token: usdc_addr,
        deposit,
        client_signer: Arc::clone(&client_signer),
        voucher_domain: voucher_domain(CHAIN_ID, payment_channel),
        prior_nonce: U256::ZERO,
        prior_bytes_delivered: U256::ZERO,
        prior_amount: U256::ZERO,
        client_binding: None,
    };
    let got = stream_fetch(
        &client_ep,
        target.clone(),
        &ctx1,
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
        "channel 1 delivery mismatch"
    );

    // GAP 1: the redeemer normalizes the voucher `v`-byte and submits
    // `withdraw`; the contract's ECDSA.recover accepts it and withdrawnAmount
    // advances on-chain.
    let withdrawn = poll_until(Duration::from_secs(60), || {
        let pc = pc_read.clone();
        async move {
            pc.getChannel(id1)
                .call()
                .await
                .ok()
                .filter(|ch| ch.withdrawnAmount > U256::ZERO)
                .map(|ch| ch.withdrawnAmount)
        }
    })
    .await;
    anyhow::ensure!(
        withdrawn.is_some(),
        "on-chain withdraw never landed (normalized v-byte not accepted, or watcher missed open)"
    );

    // FeeRouter routing: the withdraw routed its byte delta into the operator's
    // per-epoch counter. The redeemer withdraws the channel's LATEST persisted
    // voucher (`try_redeem` reads `store.get(...).last_amount`), not a specific
    // interval voucher. With two vouchers produced for this 1.5 MiB stream (the
    // 1 MiB interval voucher, then the 1.5 MiB closing one), which one the
    // single redeem lands is timing-dependent, so the routed byte total is
    // either 1 MiB or 1.5 MiB — assert it incremented, not an exact value.
    let epoch = current_epoch(&node_provider).await?;
    let routed = fee_view.bytesPerEpoch(node_addr, epoch).call().await?;
    anyhow::ensure!(
        routed > U256::ZERO,
        "FeeRouter.bytesPerEpoch did not increment (settlement not routed): {routed}"
    );

    // ============================================================
    // CHANNEL 2 — below-threshold claim, shutdown close, then settle.
    // ============================================================
    pc_client
        .openChannel(node_addr, deposit, Address::ZERO)
        .send()
        .await?
        .get_receipt()
        .await?;
    let id2 = derive_channel_id(client_addr, node_addr, 1);
    anyhow::ensure!(
        poll_until(Duration::from_secs(60), || {
            let store = Arc::clone(&store);
            async move { store.get(id2).ok().flatten() }
        })
        .await
        .is_some(),
        "watcher did not persist channel 2"
    );

    // Deliver only the 0.5 MiB suffix → ~5 µUSDC claim (< 10 µUSDC threshold),
    // so the redeemer leaves it un-redeemed for the shutdown-close path.
    let offset = (MIB) as u64; // deliver bytes [1 MiB .. 1.5 MiB)
    let ctx2 = ChannelContext {
        channel_id: id2,
        token: usdc_addr,
        deposit,
        client_signer: Arc::clone(&client_signer),
        voucher_domain: voucher_domain(CHAIN_ID, payment_channel),
        prior_nonce: U256::ZERO,
        prior_bytes_delivered: U256::ZERO,
        prior_amount: U256::ZERO,
        client_binding: None,
    };
    let suffix = stream_fetch(
        &client_ep,
        target.clone(),
        &ctx2,
        &domains.slash,
        node_addr,
        *hash.as_bytes(),
        offset,
        0x00c0_ffe2,
        Duration::from_secs(30),
    )
    .await?;
    anyhow::ensure!(
        suffix.as_ref() == &payload[MIB..],
        "channel 2 suffix mismatch"
    );

    // It must NOT have been auto-redeemed (claim below threshold).
    tokio::time::sleep(Duration::from_secs(2)).await;
    let ch2 = pc_read.getChannel(id2).call().await?;
    anyhow::ensure!(
        ch2.withdrawnAmount == U256::ZERO,
        "channel 2 should be below the redeem threshold, but was withdrawn"
    );

    // Close the un-redeemed channel 2 directly on-chain WITHOUT a voucher (so
    // the on-chain watermark stays `claimedNonce == 0 / claimedAmount == 0`,
    // strictly staler than the node's persisted nonce-1 voucher). This
    // is the exact "stale close" the self-defense dispute reaction (#1586)
    // targets. The settlement watcher stays ALIVE so it can both react with
    // `disputeChannel` and (below) observe the live `ChannelSettled`. The
    // service-driven shutdown-close path is exercised after GAP 2 (it now
    // cancels the watcher, so it cannot run before an assertion that needs the
    // watcher). `withdraw` on channel 1 above already proved a persisted voucher
    // is accepted on-chain.
    pc_read
        .closeChannelWithoutVoucher(id2)
        .send()
        .await?
        .get_receipt()
        .await?;
    let closing = poll_until(Duration::from_secs(30), || {
        let pc = pc_read.clone();
        async move {
            pc.getChannel(id2)
                .call()
                .await
                .ok()
                .filter(|ch| matches!(ch.status, PaymentChannel::Status::Closing))
        }
    })
    .await;
    anyhow::ensure!(
        closing.is_some(),
        "closeChannelWithoutVoucher did not move channel 2 to Closing"
    );

    // #1586 — the watcher observes `ChannelCloseInitiated`, sees its persisted
    // nonce-1 voucher out-ranks the stale on-chain `claimedNonce == 0`, and (the
    // few-µUSDC residual clearing the tiny `DISPUTE_MIN_RESIDUAL_MICRO_USDC`
    // floor) reacts with `disputeChannel` INSIDE the window — ratcheting the
    // on-chain claim up to the node's voucher before we warp past the deadline.
    let disputed = poll_until(Duration::from_secs(60), || {
        let pc = pc_read.clone();
        async move {
            pc.getChannel(id2)
                .call()
                .await
                .ok()
                .filter(|ch| ch.claimedNonce == U256::from(1u64))
        }
    })
    .await;
    let disputed = disputed
        .context("watcher did not self-defense-dispute the stale channel-2 close (#1586)")?;
    // Assert against the node's actual persisted voucher amount rather than a
    // hardcoded figure: the exact µUSDC depends on voucher-interval rounding of
    // the 0.5 MiB claim, and the point of the check is that the dispute ratcheted
    // the stale on-chain `claimedAmount == 0` up to the node's voucher.
    let node_voucher_amount = store
        .get(id2)?
        .context("channel 2 state missing from store")?
        .last_amount();
    anyhow::ensure!(
        !node_voucher_amount.is_zero() && disputed.claimedAmount == node_voucher_amount,
        "dispute must ratchet channel 2's claim to the node's voucher amount {node_voucher_amount}, got {}",
        disputed.claimedAmount
    );

    // Advance past the dispute window and settle (callable by anyone).
    let _: serde_json::Value = node_provider
        // Pass a plain `u64` (serializes to a JSON number); a `U256` would
        // serialize to a hex-quantity string, which not every EVM client
        // accepts for this RPC.
        .raw_request("evm_increaseTime".into(), (DISPUTE_WINDOW_SECS + 600,))
        .await?;
    let _: serde_json::Value = node_provider.raw_request("evm_mine".into(), ()).await?;
    pc_settle
        .settleChannel(id2)
        .send()
        .await?
        .get_receipt()
        .await?;

    // GAP 2: the watcher decodes ChannelSettled (provider == self) and forgets
    // the channel.
    let forgotten = poll_until(Duration::from_secs(60), || {
        let store = Arc::clone(&store);
        async move {
            match store.get(id2) {
                Ok(None) => Some(()),
                _ => None,
            }
        }
    })
    .await;
    anyhow::ensure!(
        forgotten.is_some(),
        "watcher did not observe ChannelSettled / forget channel 2 (live event-decode gap)"
    );

    // ============================================================
    // BUYER PATH (#744) — the `client` account drives a `BuyerChannelService`
    // against the registered `node_addr` provider: open → deliver (service
    // ChannelContext) → reuse → reclaimExpired. This exercises the buyer-side
    // bindings (`openChannel`, `clientChannelNonce`, `reclaimExpired`) and the
    // service end-to-end on a live deployment.
    // ============================================================
    // The buyer service runs under its OWN eth identity, distinct from the
    // `client_addr` that drove the seller-path channels above. This is what makes
    // the bootstrap reconciliation scan (#763) deterministic here: reconcile
    // re-hydrates `ChannelOpened(client == self)` orphans, and a fresh buyer
    // address owns no prior channels, so the scan finds nothing to race against
    // the lazy open below. (Reusing `client_addr` would let reconcile re-hydrate
    // the still-Open, already-withdrawn `id1` and the open would reuse it.)
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
    // (#904) — the reverting-reclaim regression scenario below depends on it.
    let buyer_provider = ProviderBuilder::new()
        .with_simple_nonce_management()
        .wallet(EthereumWallet::from((*buyer_signer).clone()))
        .connect_http(url.clone());
    let usdc_buyer = Erc20::new(usdc_addr, buyer_provider.clone());
    // Fund the buyer generously: the existing path opens 1 channel + a top-up,
    // and the #763 scenarios appended at the end open several more.
    usdc_admin
        .mint(
            buyer_addr,
            U256::from(DEPOSIT_MICRO_USDC) * U256::from(12u64),
        )
        .send()
        .await?
        .get_receipt()
        .await?;

    let buyer_store = Arc::new(MemoryBuyerChannelStore::new());
    let buyer_store_dyn: Arc<dyn BuyerChannelStore> = buyer_store.clone();
    let buyer_service = BuyerChannelService::bootstrap(
        buyer_provider.clone(),
        payment_channel,
        buyer_addr,
        buyer_store_dyn,
        Arc::clone(&buyer_signer),
        voucher_domain(CHAIN_ID, payment_channel),
        U256::from(DEPOSIT_MICRO_USDC),            // initial_deposit
        U256::from(DEPOSIT_MICRO_USDC), // working_deposit (same size: graduation not under test here)
        true, // fresh buyer identity → issue the one-time max USDC approval
        None, // idle-reconcile sweep not under test here
        Arc::new(MemoryPendingSettleStore::new()), // buyer pending-settle set (#988); unused with reconcile off
        Arc::new(Metrics::new()),
    )
    .await?;

    // Lazy open against the provider → a fresh on-chain channel (the buyer's
    // first, nonce 0) + a persisted buyer record.
    let buyer_ctx = buyer_service
        .open_or_reuse_channel(node_addr, U256::from(DEPOSIT_MICRO_USDC), OPEN_BUDGET)
        .await?;
    let buyer_id = buyer_ctx.channel_id;
    let on_chain = pc_read.getChannel(buyer_id).call().await?;
    anyhow::ensure!(
        on_chain.client == buyer_addr && on_chain.provider == node_addr,
        "buyer channel opened with wrong client/provider"
    );
    anyhow::ensure!(
        buyer_store.len() == 1,
        "buyer service should track exactly one channel after open"
    );

    // Wait for the seller watcher to persist this ChannelOpened so the handler
    // accepts the buyer's vouchers, then deliver the 0.5 MiB suffix
    // (below the redeem threshold, so the channel stays Open + un-withdrawn for
    // the reclaim assertion). The voucher is signed via the service-produced
    // ChannelContext — proving the buyer open → sign path end-to-end.
    anyhow::ensure!(
        poll_until(Duration::from_secs(60), || {
            let store = Arc::clone(&store);
            async move { store.get(buyer_id).ok().flatten() }
        })
        .await
        .is_some(),
        "watcher did not persist the buyer channel"
    );
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
        "buyer channel suffix delivery mismatch"
    );

    // Reuse: a second open for the same provider returns the SAME channel (no
    // new `openChannel`), resuming from the cumulative totals the service was
    // told to record.
    buyer_service.record_progress(
        node_addr,
        buyer_id,
        U256::from(1u64),
        U256::from(MIB / 2),
        U256::from(5u64),
    )?;
    let reuse_ctx = buyer_service
        .open_or_reuse_channel(node_addr, U256::from(DEPOSIT_MICRO_USDC), OPEN_BUDGET)
        .await?;
    anyhow::ensure!(
        reuse_ctx.channel_id == buyer_id,
        "open_or_reuse must reuse the live channel, not open a new one"
    );
    anyhow::ensure!(
        reuse_ctx.prior_nonce == U256::from(1u64),
        "reused context must resume from recorded progress"
    );
    anyhow::ensure!(
        buyer_store.len() == 1,
        "reuse must not open a second channel"
    );

    // Top-up: add funds to the live channel and assert both the on-chain
    // deposit and the persisted record reflect it.
    buyer_service
        .top_up(node_addr, U256::from(TOPUP_MICRO_USDC))
        .await?;
    let topped_deposit = U256::from(DEPOSIT_MICRO_USDC) + U256::from(TOPUP_MICRO_USDC);
    anyhow::ensure!(
        pc_read.getChannel(buyer_id).call().await?.deposit == topped_deposit,
        "topUp must raise the on-chain deposit"
    );
    anyhow::ensure!(
        buyer_store
            .get_by_provider(node_addr)?
            .ok_or_else(|| anyhow::anyhow!("buyer channel vanished after top_up"))?
            .deposit
            == topped_deposit,
        "top_up must persist the new deposit in the buyer record"
    );

    // Reclaim: force the channel to look expired to the sweep and warp the
    // chain past its on-chain expiry, then run one reclaim pass. The full
    // deposit (no withdrawal occurred) refunds to the client and the record is
    // dropped.
    let mut expired = buyer_store
        .get_by_provider(node_addr)?
        .ok_or_else(|| anyhow::anyhow!("buyer channel vanished before reclaim"))?;
    expired.expires_at = 1; // far in the past vs the system clock → sweep treats as expired
    buyer_store.record(&expired)?;
    let balance_before = usdc_buyer.balanceOf(buyer_addr).call().await?;
    let _: serde_json::Value = node_provider
        .raw_request("evm_increaseTime".into(), (CHANNEL_EXPIRY_WARP_SECS,))
        .await?;
    let _: serde_json::Value = node_provider.raw_request("evm_mine".into(), ()).await?;

    buyer_service.sweep_expired_once().await;

    anyhow::ensure!(
        buyer_store.get_by_provider(node_addr)?.is_none(),
        "reclaimed buyer channel record must be dropped"
    );
    let balance_after = usdc_buyer.balanceOf(buyer_addr).call().await?;
    anyhow::ensure!(
        balance_after.saturating_sub(balance_before) == topped_deposit,
        "reclaimExpired must refund the full deposit ({topped_deposit} µUSDC); \
         got {balance_before} → {balance_after}"
    );
    let reclaimed = pc_read.getChannel(buyer_id).call().await?;
    anyhow::ensure!(
        matches!(reclaimed.status, PaymentChannel::Status::Closed),
        "reclaimed channel must be Closed on-chain"
    );

    // Retire the original seller `service` now, via its graceful-shutdown path
    // (quiesce redeemer + flush checkpoint + stop the watcher). It has served
    // its purpose — channel 2's settle observation and the buyer channel's
    // ChannelOpened persistence are done — and channel 3 below is persisted by
    // its OWN dedicated service, so the original watcher is no longer needed.
    // Retiring it here (rather than at channel 2's close, which now runs on-chain
    // directly so the watcher can survive to observe the settle) is what keeps
    // the original redeemer from racing the auto-settle service on channel 3.
    service
        .close_open_channels_on_shutdown(Duration::from_secs(30))
        .await;

    // ============================================================
    // CHANNEL 3 — AUTO-SETTLEMENT (#742). Bring up a dedicated settlement
    // service opted in to auto-settle via a small `value_threshold`, sharing the
    // same store as the live handler so its watcher persists the channel and its
    // redeemer can read the handler-persisted voucher. The original `service`'s
    // redeemer was permanently quiesced by the
    // `close_open_channels_on_shutdown` just above, and the handler's
    // `redeem_hint` sender is wired at construction, bound to that dead service —
    // so we drive this service's redeemer directly via its OWN
    // `redeem_hint_sender()` after the
    // delivery persists the voucher. Deliver enough to cross BOTH the redeem
    // threshold (10 µUSDC) AND the auto-settle value threshold (5 µUSDC), and
    // assert auto-settle SUPERSEDES `withdraw`: the channel goes `Closing` (not
    // withdrawn), a `PendingSettle` entry is recorded for the settle sweep, and
    // the channel is forgotten from the store (fix #1 — no unredeemable-bytes
    // leak: we stop serving a `Closing` channel).
    // ============================================================
    let auto_settle_store: Arc<dyn PendingSettleStore> = concrete_store.clone();
    let (auto_redeem_tx, auto_redeem_rx) =
        tokio::sync::mpsc::channel(decdn_node::payment_settlement::REDEEM_HINT_CAPACITY);
    let service_auto = PaymentChannelService::bootstrap(
        node_provider.clone(),
        payment_channel,
        node_addr,
        Arc::clone(&store),
        concrete_store.clone(),
        concrete_store.clone(),
        Arc::clone(&handler),
        U256::from(REDEEM_THRESHOLD_MICRO_USDC),
        Duration::from_secs(300),
        AutoSettleConfig {
            // 5 µUSDC — below the 10 µUSDC redeem threshold and below channel
            // 3's 15 µUSDC claim, so the trigger fires AND the redeem threshold
            // is also crossed (proving auto-settle wins).
            value_threshold: Some(U256::from(5u64)),
            voucher_nonce_span_threshold: None,
        },
        U256::from(DISPUTE_MIN_RESIDUAL_MICRO_USDC),
        Duration::from_millis(250),
        e2e_head(&node_provider),
        Arc::clone(&metrics),
        auto_redeem_tx,
        auto_redeem_rx,
    )
    .await?;
    let auto_hint = service_auto.redeem_hint_sender();

    // Re-fund + re-approve, then open channel 3.
    usdc_admin
        .mint(client_addr, U256::from(DEPOSIT_MICRO_USDC))
        .send()
        .await?
        .get_receipt()
        .await?;
    usdc_client
        .approve(payment_channel, U256::from(DEPOSIT_MICRO_USDC))
        .send()
        .await?
        .get_receipt()
        .await?;
    let auto_nonce = pc_read.clientChannelNonce(client_addr).call().await?;
    let id3 = derive_channel_id(client_addr, node_addr, auto_nonce.to::<u64>());
    pc_client
        .openChannel(node_addr, deposit, Address::ZERO)
        .send()
        .await?
        .get_receipt()
        .await?;
    anyhow::ensure!(
        poll_until(Duration::from_secs(60), || {
            let store = Arc::clone(&store);
            async move { store.get(id3).ok().flatten() }
        })
        .await
        .is_some(),
        "watcher did not persist channel 3 (auto-settle)"
    );

    // Deliver the full 1.5 MiB blob → 15 µUSDC claim, above both thresholds.
    let ctx3 = ChannelContext {
        channel_id: id3,
        token: usdc_addr,
        deposit,
        client_signer: Arc::clone(&client_signer),
        voucher_domain: voucher_domain(CHAIN_ID, payment_channel),
        prior_nonce: U256::ZERO,
        prior_bytes_delivered: U256::ZERO,
        prior_amount: U256::ZERO,
        client_binding: None,
    };
    let got3 = stream_fetch(
        &client_ep,
        target.clone(),
        &ctx3,
        &domains.slash,
        node_addr,
        *hash.as_bytes(),
        0,
        0x00c0_ffe4,
        Duration::from_secs(30),
    )
    .await?;
    anyhow::ensure!(
        got3.as_ref() == payload.as_slice(),
        "channel 3 delivery mismatch"
    );

    // Wait for the handler's serve path to persist channel 3's voucher into the
    // shared store, then hint the auto-settle service's redeemer (the handler's
    // construction-wired hint is bound to the now-dead original service, so we drive this
    // redeemer directly via its own sender). The redeemer reads the persisted
    // voucher from the same store.
    anyhow::ensure!(
        poll_until(Duration::from_secs(30), || {
            let store = Arc::clone(&store);
            async move {
                store
                    .get(id3)
                    .ok()
                    .flatten()
                    .filter(|st| !st.last_nonce().is_zero())
            }
        })
        .await
        .is_some(),
        "channel 3 voucher was never persisted by the handler serve path"
    );
    auto_hint.send(id3).await?;

    // The auto-settle trigger fires before `withdraw`: the channel goes
    // `Closing` and `withdrawnAmount` stays zero (auto-settle SUPERSEDES the
    // withdraw — fix #4b).
    let closing3 = poll_until(Duration::from_secs(60), || {
        let pc = pc_read.clone();
        async move {
            pc.getChannel(id3)
                .call()
                .await
                .ok()
                .filter(|ch| matches!(ch.status, PaymentChannel::Status::Closing))
        }
    })
    .await
    .ok_or_else(|| anyhow::anyhow!("auto-settlement did not move channel 3 to Closing (#742)"))?;
    anyhow::ensure!(
        closing3.withdrawnAmount == U256::ZERO,
        "auto-settle must close (not withdraw) — withdrawnAmount should be 0, got {}",
        closing3.withdrawnAmount
    );

    // A `PendingSettle` entry was recorded so the settle sweep finalizes the
    // remainder after the dispute window (mirrors the shutdown-close path).
    let pending3 = poll_until(Duration::from_secs(30), || {
        let pending = Arc::clone(&auto_settle_store);
        async move {
            pending
                .load_pending()
                .ok()
                .filter(|entries| entries.iter().any(|e| e.channel_id == id3))
        }
    })
    .await;
    anyhow::ensure!(
        pending3.is_some(),
        "auto-settle close did not record a PendingSettle entry for channel 3"
    );

    // Fix #1: after the landed close the channel is RETIRED — forgotten from the
    // store so the node stops serving a channel it can no longer redeem against.
    let forgotten3 = poll_until(Duration::from_secs(30), || {
        let store = Arc::clone(&store);
        async move {
            match store.get(id3) {
                Ok(None) => Some(()),
                _ => None,
            }
        }
    })
    .await;
    anyhow::ensure!(
        forgotten3.is_some(),
        "auto-settle close did not forget channel 3 — unredeemable-bytes leak (fix #1)"
    );

    // The success counter incremented and no failure was recorded (the close
    // landed cleanly).
    let metrics_text = metrics.encode()?;
    anyhow::ensure!(
        metrics_text
            .lines()
            .any(|l| l == "decdn_settlement_auto_triggered_total 1"),
        "expected one secured auto-settle close in metrics:\n{metrics_text}"
    );
    anyhow::ensure!(
        metrics_text
            .lines()
            .any(|l| l == "decdn_settlement_auto_failures_total 0"),
        "expected zero auto-settle failures in metrics:\n{metrics_text}"
    );

    drop(service_auto);

    // ============================================================
    // DOWNTIME BACKFILL (#751) — the headline across-restart fix. Take the
    // settlement service DOWN (drop it → watcher/sweeper aborted), open a fresh
    // channel against this provider while nothing is watching, then bring the
    // node back up by re-bootstrapping a second service against the SAME store.
    // Service 2's bootstrap reads the scan checkpoint service 1 persisted, floors
    // the backfill below the new channel's block, and registers it — proving a
    // channel opened during downtime is recovered, not rejected `WrongChannel`
    // forever.
    // ============================================================
    drop(service); // node "goes down": watcher/sweeper/redeemer all aborted

    // Re-fund + re-approve so the deposit is covered regardless of prior spend
    // (ERC-20 `approve` overwrites the allowance), then open the channel while no
    // service is watching. Derive the channel id from the live per-client nonce
    // (read before the open) rather than hardcoding it, so the proof survives any
    // change to the earlier channel count.
    usdc_admin
        .mint(
            client_addr,
            U256::from(DEPOSIT_MICRO_USDC) * U256::from(2u64),
        )
        .send()
        .await?
        .get_receipt()
        .await?;
    usdc_client
        .approve(
            payment_channel,
            U256::from(DEPOSIT_MICRO_USDC) * U256::from(2u64),
        )
        .send()
        .await?
        .get_receipt()
        .await?;
    let down_nonce = pc_read.clientChannelNonce(client_addr).call().await?;
    let down_id = derive_channel_id(client_addr, node_addr, down_nonce.to::<u64>());
    pc_client
        .openChannel(node_addr, U256::from(DEPOSIT_MICRO_USDC), Address::ZERO)
        .send()
        .await?
        .get_receipt()
        .await?;
    let down_block = node_provider.get_block_number().await?;
    // While down, nothing registered it.
    anyhow::ensure!(
        store.get(down_id)?.is_none(),
        "channel opened while the service is down must be unknown until re-bootstrap"
    );

    // Mine a block so service2's bootstrap head is STRICTLY above the downtime
    // channel's block. This is what makes the proof specific to #751: the #762
    // within-session backfill floors at the bootstrap head and scans
    // `[head, F]`, which now EXCLUDES `down_block` — so only the persisted
    // checkpoint floor (#751) can pull the backfill start low enough to re-cover
    // it. Without this, `head == down_block` and the test would pass on the #762
    // path alone, proving nothing.
    let _: serde_json::Value = node_provider.raw_request("evm_mine".into(), ()).await?;
    anyhow::ensure!(
        node_provider.get_block_number().await? > down_block,
        "evm_mine must advance head above the downtime channel block"
    );

    // Node comes back up against the same store. Bootstrap re-reads the persisted
    // checkpoint and the bring-up backfill covers the downtime block.
    let (restart_redeem_sender, restart_redeem_receiver) =
        tokio::sync::mpsc::channel(decdn_node::payment_settlement::REDEEM_HINT_CAPACITY);
    let service2 = PaymentChannelService::bootstrap(
        node_provider.clone(),
        payment_channel,
        node_addr,
        Arc::clone(&store),
        concrete_store.clone(),
        concrete_store.clone(),
        Arc::clone(&handler),
        U256::from(REDEEM_THRESHOLD_MICRO_USDC),
        Duration::from_secs(300),
        AutoSettleConfig::default(),
        U256::from(DISPUTE_MIN_RESIDUAL_MICRO_USDC),
        Duration::from_millis(250),
        e2e_head(&node_provider),
        Arc::clone(&metrics),
        restart_redeem_sender,
        restart_redeem_receiver,
    )
    .await?;
    let backfilled = poll_until(Duration::from_secs(60), || {
        let store = Arc::clone(&store);
        async move { store.get(down_id).ok().flatten() }
    })
    .await;
    anyhow::ensure!(
        backfilled.is_some(),
        "downtime-opened channel was not registered after restart — #751 checkpoint backfill failed"
    );
    drop(service2);
    // `AbortOnDrop` aborts asynchronously, so let the aborted watcher fully wind
    // down before the no-pending precondition below — otherwise a lingering
    // service2 tick could race the upcoming open+close.
    tokio::task::yield_now().await;

    // ============================================================
    // CLOSING RECONCILIATION (#839) — two gaps the fix closes. First (boot scan):
    // a channel this node provides is left `Closing` on-chain with NO local
    // `PendingSettle` — the crash gap between a landed `closeChannel` and the
    // durable `record_pending` write — produced here by closing directly via the
    // contract while no settlement service is alive (service + service2 dropped).
    // A fresh bootstrap's closing-reconciliation backfill must re-derive the
    // `PendingSettle` (stamped with the on-chain `disputeDeadline`) so the settle
    // sweep can finalize the channel (settle its claim and clear the obligation),
    // and the live `ChannelSettled` arm must then drop the recovered entry.
    // Second (live arm, below): a *client*-initiated close against the running
    // service must be recorded by the live `ChannelCloseInitiated` arm. The
    // channels here are never drawn, so settlement routes a zero remainder and
    // refunds the deposit — the path under test is obligation recovery, not
    // payout.
    // ============================================================
    // The downtime phase minted+approved 2×DEPOSIT and spent 1×DEPOSIT on
    // `down_id`, so a 1×DEPOSIT allowance + balance remains for this open.
    let close_nonce = pc_read.clientChannelNonce(client_addr).call().await?;
    let close_id = derive_channel_id(client_addr, node_addr, close_nonce.to::<u64>());
    pc_client
        .openChannel(node_addr, U256::from(DEPOSIT_MICRO_USDC), Address::ZERO)
        .send()
        .await?
        .get_receipt()
        .await?;
    // Close it directly via the contract (no voucher: the channel was never
    // drawn, so `claimedNonce == 0`). Sent from the node wallet (`pc_read`
    // is node-provider-filled), matching the node's own crashed close: the
    // channel is `Closing` on-chain but no service recorded a pending entry.
    pc_read
        .closeChannelWithoutVoucher(close_id)
        .send()
        .await?
        .get_receipt()
        .await?;
    let closed_ch = pc_read.getChannel(close_id).call().await?;
    anyhow::ensure!(
        matches!(closed_ch.status, PaymentChannel::Status::Closing),
        "direct closeChannelWithoutVoucher did not move the reconciliation channel to Closing"
    );
    // No service was watching, so the obligation is absent before re-bootstrap —
    // the very gap #839 recovers.
    anyhow::ensure!(
        concrete_store
            .load_pending()?
            .iter()
            .all(|e| e.channel_id != close_id),
        "a Closing channel closed while down must have no PendingSettle until re-bootstrap"
    );

    // Node comes back up against the same store: the bring-up closing-
    // reconciliation backfill must record the obligation.
    let (close_redeem_sender, close_redeem_receiver) =
        tokio::sync::mpsc::channel(decdn_node::payment_settlement::REDEEM_HINT_CAPACITY);
    let service3 = PaymentChannelService::bootstrap(
        node_provider.clone(),
        payment_channel,
        node_addr,
        Arc::clone(&store),
        concrete_store.clone(),
        concrete_store.clone(),
        Arc::clone(&handler),
        U256::from(REDEEM_THRESHOLD_MICRO_USDC),
        Duration::from_secs(300),
        AutoSettleConfig::default(),
        U256::from(DISPUTE_MIN_RESIDUAL_MICRO_USDC),
        Duration::from_millis(250),
        e2e_head(&node_provider),
        Arc::clone(&metrics),
        close_redeem_sender,
        close_redeem_receiver,
    )
    .await?;
    let recovered = poll_until(Duration::from_secs(60), || {
        let pending = concrete_store.clone();
        async move {
            pending
                .load_pending()
                .ok()
                .and_then(|entries| entries.into_iter().find(|e| e.channel_id == close_id))
        }
    })
    .await
    .ok_or_else(|| {
        anyhow::anyhow!("closing-reconciliation backfill did not recover the PendingSettle (#839)")
    })?;
    anyhow::ensure!(
        recovered.settle_after == closed_ch.disputeDeadline,
        "recovered PendingSettle deadline {} must equal the on-chain disputeDeadline {}",
        recovered.settle_after,
        closed_ch.disputeDeadline
    );

    // The recovered entry is actionable end-to-end: warp past the dispute window,
    // settle (callable by anyone), and the live ChannelSettled arm drops the
    // recovered pending entry.
    let _: serde_json::Value = node_provider
        .raw_request("evm_increaseTime".into(), (DISPUTE_WINDOW_SECS + 600,))
        .await?;
    let _: serde_json::Value = node_provider.raw_request("evm_mine".into(), ()).await?;
    pc_settle
        .settleChannel(close_id)
        .send()
        .await?
        .get_receipt()
        .await?;
    let dropped = poll_until(Duration::from_secs(60), || {
        let pending = concrete_store.clone();
        async move {
            pending
                .load_pending()
                .ok()
                .filter(|entries| entries.iter().all(|e| e.channel_id != close_id))
                .map(|_| ())
        }
    })
    .await;
    anyhow::ensure!(
        dropped.is_some(),
        "settled reconciliation channel's recovered PendingSettle was not dropped (#839)"
    );

    // --- Live arm + client-initiated close (#839, second gap) ---
    // With service3 running, the CLIENT closes a channel this node provides. The
    // node's own close path never fires, so only the live `ChannelCloseInitiated`
    // arm can record the obligation. A full client-wallet binding is needed
    // because `pc_client` (PaymentChannelOpen) exposes only `openChannel`.
    let pc_client_full = PaymentChannel::new(payment_channel, client_provider.clone());
    usdc_admin
        .mint(client_addr, U256::from(DEPOSIT_MICRO_USDC))
        .send()
        .await?
        .get_receipt()
        .await?;
    usdc_client
        .approve(payment_channel, U256::from(DEPOSIT_MICRO_USDC))
        .send()
        .await?
        .get_receipt()
        .await?;
    let live_nonce = pc_read.clientChannelNonce(client_addr).call().await?;
    let live_id = derive_channel_id(client_addr, node_addr, live_nonce.to::<u64>());
    pc_client
        .openChannel(node_addr, U256::from(DEPOSIT_MICRO_USDC), Address::ZERO)
        .send()
        .await?
        .get_receipt()
        .await?;
    // Let service3's watcher register the open first, so the subsequent close is
    // an unambiguous live event against a known channel.
    anyhow::ensure!(
        poll_until(Duration::from_secs(60), || {
            let store = Arc::clone(&store);
            async move { store.get(live_id).ok().flatten() }
        })
        .await
        .is_some(),
        "watcher did not register the live-arm channel open"
    );
    // Client-initiated voucher-less close (never drawn → claimedNonce == 0).
    pc_client_full
        .closeChannelWithoutVoucher(live_id)
        .send()
        .await?
        .get_receipt()
        .await?;
    let live_recovered = poll_until(Duration::from_secs(60), || {
        let pending = concrete_store.clone();
        async move {
            pending
                .load_pending()
                .ok()
                .and_then(|entries| entries.into_iter().find(|e| e.channel_id == live_id))
        }
    })
    .await
    .ok_or_else(|| {
        anyhow::anyhow!(
            "live ChannelCloseInitiated arm did not record a PendingSettle for a client-initiated close (#839)"
        )
    })?;
    let live_ch = pc_read.getChannel(live_id).call().await?;
    anyhow::ensure!(
        live_recovered.settle_after == live_ch.disputeDeadline,
        "live-arm PendingSettle deadline {} must equal the on-chain disputeDeadline {}",
        live_recovered.settle_after,
        live_ch.disputeDeadline
    );
    // Pin the recovery to the *direct* reconcile path, not the re-arm+resubscribe
    // failure path (whose longer round-trip the 60s poll above could otherwise
    // mask): no watcher persist/reconcile failure was recorded across the whole
    // run. Mirrors the auto-settle block's `_auto_failures_total 0` guard.
    let watcher_metrics = metrics.encode()?;
    anyhow::ensure!(
        watcher_metrics
            .lines()
            .any(|l| l == "decdn_watcher_persist_failures_total 0"),
        "live-arm recovery must use the clean reconcile path (no watcher persist failures):\n{watcher_metrics}"
    );
    drop(service3);

    // ============================================================
    // #763 BUYER ROBUSTNESS — concurrency, expired rotation, and bootstrap
    // reconciliation, all under the dedicated `buyer_addr` identity so the
    // reconcile scan only ever sees the buyer's own channels. No seller
    // settlement service is alive here; these scenarios are purely on-chain
    // (open / reclaim) + the chain-log reconciliation scan, so none is needed.
    // ============================================================

    // --- A0. An abandoned open still lands, and the next pull reuses it (#1143). ---
    // Anvil mines instantly, so every other open here resolves well inside
    // OPEN_BUDGET and the timeout arm of `open_or_reuse_channel` is never reached:
    // the singleflight's whole reason for existing goes unexercised against a real
    // chain. Turn automine OFF so the `openChannel` genuinely sits in the mempool
    // — which is exactly the state in which releasing the provider's slot would let
    // a second deposit through.
    let _: serde_json::Value = node_provider
        .raw_request("anvil_setAutomine".into(), (false,))
        .await?;
    let pend_nonce_before = pc_read.clientChannelNonce(buyer_addr).call().await?;
    let pending_err = buyer_service
        .open_or_reuse_channel(
            node_addr,
            U256::from(DEPOSIT_MICRO_USDC),
            Duration::from_secs(1),
        )
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("an unmined openChannel cannot yield a live channel"))?;
    anyhow::ensure!(
        pending_err.downcast_ref::<ChannelOpenPending>().is_some(),
        "a caller that outran its budget must get the typed pending sentinel, not a failure: \
         {pending_err:#}"
    );
    anyhow::ensure!(
        buyer_store.get_by_provider(node_addr)?.is_none(),
        "nothing may be persisted while the open is still in the mempool"
    );
    // Mine it. The caller is long gone; the detached task still owns the tx and the
    // provider's open slot, so it is the one that collects the receipt and persists.
    let _: serde_json::Value = node_provider
        .raw_request("anvil_setAutomine".into(), (true,))
        .await?;
    let _: serde_json::Value = node_provider.raw_request("evm_mine".into(), ()).await?;
    let landed = poll_until(Duration::from_secs(60), || {
        let store = Arc::clone(&buyer_store);
        async move { store.get_by_provider(node_addr).ok().flatten() }
    })
    .await
    .ok_or_else(|| {
        anyhow::anyhow!(
            "the detached open task never persisted the channel the caller walked away from"
        )
    })?;
    let pend_nonce_after = pc_read.clientChannelNonce(buyer_addr).call().await?;
    anyhow::ensure!(
        pend_nonce_after == pend_nonce_before + U256::from(1u64),
        "the abandoned open must escrow exactly ONE channel (nonce {pend_nonce_before} → \
         {pend_nonce_after})"
    );
    let reused = buyer_service
        .open_or_reuse_channel(node_addr, U256::from(DEPOSIT_MICRO_USDC), OPEN_BUDGET)
        .await?;
    anyhow::ensure!(
        reused.channel_id == landed.channel_id && buyer_store.len() == 1,
        "the next pull must reuse the channel the detached task landed, not open a second"
    );
    // Clear it again so A below still starts from an untracked provider.
    let mut spent = landed;
    spent.expires_at = 1;
    buyer_store.record(&spent)?;
    let _: serde_json::Value = node_provider
        .raw_request("evm_increaseTime".into(), (CHANNEL_EXPIRY_WARP_SECS,))
        .await?;
    let _: serde_json::Value = node_provider.raw_request("evm_mine".into(), ()).await?;
    buyer_service.sweep_expired_once().await;

    // --- A. Concurrent same-provider opens escrow exactly one deposit (#753). ---
    // Two racing open_or_reuse for one provider. Since #1143 the loser does not
    // bail for retry — it JOINS the winner's in-flight open and receives the same
    // channel — so both calls now succeed. What must not change is the invariant:
    // the on-chain client nonce advances by EXACTLY one and only one channel is
    // ever tracked. Two deposits for one provider would orphan the first, and the
    // boot reconcile scan deliberately declines to adopt it.
    anyhow::ensure!(
        buyer_store.get_by_provider(node_addr)?.is_none(),
        "precondition: no buyer channel tracked for the provider after the reclaim above"
    );
    let nonce_before = pc_read.clientChannelNonce(buyer_addr).call().await?;
    let (r1, r2) = tokio::join!(
        buyer_service.open_or_reuse_channel(node_addr, U256::from(DEPOSIT_MICRO_USDC), OPEN_BUDGET),
        buyer_service.open_or_reuse_channel(node_addr, U256::from(DEPOSIT_MICRO_USDC), OPEN_BUDGET),
    );
    let nonce_after = pc_read.clientChannelNonce(buyer_addr).call().await?;
    anyhow::ensure!(
        nonce_after == nonce_before + U256::from(1u64),
        "concurrent opens must escrow exactly one channel (nonce {nonce_before} → {nonce_after})"
    );
    anyhow::ensure!(
        buyer_store.len() == 1,
        "concurrent opens must leave exactly one tracked channel"
    );
    let opened_ids: Vec<_> = [&r1, &r2]
        .into_iter()
        .filter_map(|r| r.as_ref().ok().map(|ctx| ctx.channel_id))
        .collect();
    let win_id = opened_ids
        .first()
        .copied()
        .ok_or_else(|| anyhow::anyhow!("at least one concurrent open must succeed"))?;
    anyhow::ensure!(
        opened_ids.iter().all(|id| *id == win_id),
        "concurrent opens must never surface two distinct channels"
    );
    let reuse_ctx = buyer_service
        .open_or_reuse_channel(node_addr, U256::from(DEPOSIT_MICRO_USDC), OPEN_BUDGET)
        .await?;
    anyhow::ensure!(
        reuse_ctx.channel_id == win_id && buyer_store.len() == 1,
        "a retry after the concurrent race reuses the one channel"
    );

    // --- B. Expired-channel rotation reached THROUGH open_or_reuse_channel. ---
    // Mark the tracked channel expired and warp past its on-chain expiry, then a
    // fresh open_or_reuse must reclaim it FIRST (reclaim-before-reopen, dropping
    // the old record) and open a new channel — the rotation path previously only
    // reachable via sweep_expired_once directly, not through open_or_reuse.
    let mut row = buyer_store
        .get_by_provider(node_addr)?
        .ok_or_else(|| anyhow::anyhow!("buyer channel vanished before rotation test"))?;
    row.expires_at = 1; // expired vs host clock → reclaim candidate
    buyer_store.record(&row)?;
    let _: serde_json::Value = node_provider
        .raw_request("evm_increaseTime".into(), (CHANNEL_EXPIRY_WARP_SECS,))
        .await?;
    let _: serde_json::Value = node_provider.raw_request("evm_mine".into(), ()).await?;
    let rotated = buyer_service
        .open_or_reuse_channel(node_addr, U256::from(DEPOSIT_MICRO_USDC), OPEN_BUDGET)
        .await?;
    anyhow::ensure!(
        rotated.channel_id != win_id && buyer_store.len() == 1,
        "rotation must reclaim the expired channel and open a new one"
    );
    anyhow::ensure!(
        matches!(
            pc_read.getChannel(win_id).call().await?.status,
            PaymentChannel::Status::Closed
        ),
        "the rotated-away channel must be reclaimed (Closed) on-chain"
    );

    // --- C. Bootstrap reconciliation re-hydrates an orphaned on-chain channel. ---
    // Open a channel directly via the buyer wallet so no service tracks it, then
    // bring up a fresh BuyerChannelService (empty store) under the same identity:
    // its bootstrap reconciliation scan must discover the on-chain orphan and
    // re-hydrate it so the reclaim sweep can recover the deposit.
    let pc_buyer = PaymentChannelOpen::new(payment_channel, buyer_provider.clone());
    pc_buyer
        .openChannel(node_addr, U256::from(DEPOSIT_MICRO_USDC), Address::ZERO)
        .send()
        .await?
        .get_receipt()
        .await?;
    let recon_store = Arc::new(MemoryBuyerChannelStore::new());
    let recon_store_dyn: Arc<dyn BuyerChannelStore> = recon_store.clone();
    let recon_service = BuyerChannelService::bootstrap(
        buyer_provider.clone(),
        payment_channel,
        buyer_addr,
        recon_store_dyn,
        Arc::clone(&buyer_signer),
        voucher_domain(CHAIN_ID, payment_channel),
        U256::from(DEPOSIT_MICRO_USDC), // initial_deposit
        U256::from(DEPOSIT_MICRO_USDC), // working_deposit (same size: graduation not under test here)
        false,
        None, // idle-reconcile sweep not under test here
        Arc::new(MemoryPendingSettleStore::new()),
        Arc::new(Metrics::new()),
    )
    .await?;
    let rehydrated = poll_until(Duration::from_secs(60), || {
        let recon_store = Arc::clone(&recon_store);
        async move { recon_store.get_by_provider(node_addr).ok().flatten() }
    })
    .await
    .ok_or_else(|| {
        anyhow::anyhow!("bootstrap reconciliation did not re-hydrate any orphaned buyer channel")
    })?;
    let recon_view = pc_read.getChannel(rehydrated.channel_id).call().await?;
    anyhow::ensure!(
        recon_view.client == buyer_addr
            && recon_view.provider == node_addr
            && matches!(recon_view.status, PaymentChannel::Status::Open),
        "re-hydrated channel must be an Open channel we own"
    );
    anyhow::ensure!(
        rehydrated.deposit == recon_view.deposit && rehydrated.expires_at == recon_view.expiresAt,
        "re-hydrated state must mirror the on-chain deposit/expiry"
    );
    drop(recon_service);

    // --- D. Reconciliation repairs an undecodable (post-downgrade) row, and a
    // corrupt sibling never blocks the reclaim sweep (#753 / #763). ---
    let pstore_dir = tempfile::tempdir()?;
    // `PersistentChannelStateStore::open` enforces a `0o700` data_dir; a CI umask
    // of 002 leaves the tempdir at 0o755, so tighten it (mirrors the seller store
    // setup above).
    #[cfg(unix)]
    std::fs::set_permissions(
        pstore_dir.path(),
        std::os::unix::fs::PermissionsExt::from_mode(0o700),
    )?;
    let pconcrete = Arc::new(PersistentChannelStateStore::open(pstore_dir.path())?);
    // A real on-chain channel whose local row got corrupted by a downgrade.
    let corrupt_open_receipt = pc_buyer
        .openChannel(node_addr, U256::from(DEPOSIT_MICRO_USDC), Address::ZERO)
        .send()
        .await?
        .get_receipt()
        .await?;
    let corrupt_channel_id = corrupt_open_receipt
        .inner
        .logs()
        .iter()
        .filter_map(|log| log.log_decode::<PaymentChannel::ChannelOpened>().ok())
        .map(|decoded| decoded.inner.data)
        .find(|ev| ev.client == buyer_addr && ev.provider == node_addr)
        .map(|ev| ev.channelId)
        .ok_or_else(|| anyhow::anyhow!("ChannelOpened event missing from openChannel receipt"))?;
    // Seed an undecodable value under the channel_id key (the primary key,
    // #1481) — its provider is unrecoverable from disk — exactly what
    // reconcile rebuilds from chain.
    pconcrete.insert_raw_buyer_record(corrupt_channel_id, &[0u8; 8])?;
    let pstore_dyn: Arc<dyn BuyerChannelStore> =
        Arc::new(BuyerChannelStoreHandle::new(Arc::clone(&pconcrete)));
    let persist_service = BuyerChannelService::bootstrap(
        buyer_provider.clone(),
        payment_channel,
        buyer_addr,
        Arc::clone(&pstore_dyn),
        Arc::clone(&buyer_signer),
        voucher_domain(CHAIN_ID, payment_channel),
        U256::from(DEPOSIT_MICRO_USDC), // initial_deposit
        U256::from(DEPOSIT_MICRO_USDC), // working_deposit (same size: graduation not under test here)
        false,
        None, // idle-reconcile sweep not under test here
        Arc::new(MemoryPendingSettleStore::new()),
        Arc::new(Metrics::new()),
    )
    .await?;
    let repaired = poll_until(Duration::from_secs(60), || {
        let s = Arc::clone(&pstore_dyn);
        async move { s.get_by_provider(node_addr).ok().flatten() }
    })
    .await
    .ok_or_else(|| anyhow::anyhow!("reconcile did not repair the undecodable buyer row"))?;
    anyhow::ensure!(
        matches!(
            pc_read.getChannel(repaired.channel_id).call().await?.status,
            PaymentChannel::Status::Open
        ),
        "the repaired row must point at an Open on-chain channel"
    );
    // D2 (mixed reclaim): seed an undecodable row under an unrelated channel_id
    // key alongside the healthy (now-expired) repaired row, then sweep — the
    // corrupt sibling is skipped by load_all and the healthy channel is still
    // reclaimed.
    let corrupt_channel_id_2 = B256::repeat_byte(0xCD);
    pconcrete.insert_raw_buyer_record(corrupt_channel_id_2, &[0u8; 8])?;
    let mut healthy = pstore_dyn
        .get_by_provider(node_addr)?
        .ok_or_else(|| anyhow::anyhow!("repaired row vanished before mixed-reclaim test"))?;
    healthy.expires_at = 1;
    pstore_dyn.record(&healthy)?;
    let _: serde_json::Value = node_provider
        .raw_request("evm_increaseTime".into(), (CHANNEL_EXPIRY_WARP_SECS,))
        .await?;
    let _: serde_json::Value = node_provider.raw_request("evm_mine".into(), ()).await?;
    persist_service.sweep_expired_once().await;
    anyhow::ensure!(
        pstore_dyn.get_by_provider(node_addr)?.is_none(),
        "the healthy expired channel must be reclaimed despite the corrupt sibling"
    );
    anyhow::ensure!(
        pstore_dyn.get_by_channel_id(corrupt_channel_id_2).is_err(),
        "the corrupt sibling row remains (skipped, not reclaimed)"
    );
    drop(persist_service);

    // --- E. A reverting reclaimExpired must NOT wedge the buyer tx lane (#904). ---
    // `reclaimExpired` is *expected* to revert under host-clock-vs-chain skew and
    // be retried. With alloy's default CachedNonceManager the nonce is advanced
    // when the tx is prepared; a send that then fails (its gas-estimate reverts)
    // never lands but leaves the cached nonce advanced, gapping every later tx
    // from this wallet. `get_receipt` has no default timeout, so the original
    // repro hung until the harness `OVERALL_TIMEOUT` killed the run. The buyer
    // provider uses `with_simple_nonce_management`, so the lane stays live.
    //
    // Driven at the contract layer (not the sweep) so the reclaim target is a
    // channel we just opened against the registered node — `openChannel` rejects
    // unregistered providers, and a freshly opened channel is genuinely not-yet-
    // expired (every prior scenario warped chain time past the max lifetime).
    let pc_buyer_full = PaymentChannel::new(payment_channel, buyer_provider.clone());
    let open_receipt = pc_buyer
        .openChannel(node_addr, U256::from(DEPOSIT_MICRO_USDC), Address::ZERO)
        .send()
        .await?
        .get_receipt()
        .await?;
    let regress_id = open_receipt
        .inner
        .logs()
        .iter()
        .filter_map(|log| log.log_decode::<PaymentChannel::ChannelOpened>().ok())
        .map(|decoded| decoded.inner.data)
        .find(|ev| ev.client == buyer_addr && ev.provider == node_addr)
        .map(|ev| ev.channelId)
        .ok_or_else(|| anyhow::anyhow!("ChannelOpened event missing from openChannel receipt"))?;
    // A not-yet-expired channel: reclaimExpired's gas estimate reverts, so the
    // send fails after the cached nonce was already advanced (the leak path).
    let reclaim_err = match pc_buyer_full.reclaimExpired(regress_id).send().await {
        Ok(_) => anyhow::bail!(
            "reclaimExpired on a not-yet-expired channel must revert (precondition for the regression)"
        ),
        Err(e) => format!("{e:?}").to_lowercase(),
    };
    // Assert it failed *because the call reverted*, not via an unrelated RPC or
    // signing error that would let the scenario pass without exercising the
    // gas-estimate-revert path that leaks the cached nonce.
    anyhow::ensure!(
        reclaim_err.contains("revert"),
        "reclaimExpired must fail with an on-chain revert, not an unrelated error: {reclaim_err}"
    );
    // The next buyer tx must land promptly. A leaked/gapped nonce would leave it
    // unmined indefinitely (under anvil automine nothing fills the gap), so a
    // tight bound turns the regression into a fast, named failure.
    let followup = tokio::time::timeout(Duration::from_secs(20), async {
        let receipt = pc_buyer
            .openChannel(node_addr, U256::from(DEPOSIT_MICRO_USDC), Address::ZERO)
            .send()
            .await?
            .get_receipt()
            .await?;
        anyhow::Ok(receipt)
    })
    .await
    .map_err(|_| {
        anyhow::anyhow!("buyer tx after a reverting reclaim hung — nonce lane wedged (#904)")
    })??;
    anyhow::ensure!(
        followup.status(),
        "the follow-up buyer openChannel must land on-chain after the reverting reclaim"
    );

    client_ep.close().await;
    server_ep.close().await;
    let _ = server_task.await;
    Ok(())
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

/// Read `(CapacityBond, PaymentChannel, FeeRouter, Token, SlashJudge)` from the
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
        get("PaymentChannel")?,
        get("FeeRouter")?,
        get("Token")?,
        get("SlashJudge")?,
    ))
}
