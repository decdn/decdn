//! Live anvil-backed e2e for the on-chain `PaymentChannel` seller settlement
//! path (issue #745, on top of PR #743). Gated behind the `anvil-e2e` feature
//! so the default test run stays fast and needs no `anvil`/`forge` binaries.
//!
//! PR #743 verified the seller settlement path statically (build, clippy, unit
//! tests, ABI cross-check) but never ran it against a chain. This test closes
//! the two runtime-only gaps it called out:
//!
//!   1. **On-chain acceptance of the normalized voucher signature `v`-byte** —
//!      a real client-signed voucher must be accepted by the contract's
//!      `ECDSA.recover` when the node submits `withdraw` / `closeChannel`.
//!   2. **Live event decode** — the [`PaymentChannelService`] watcher's
//!      subscribe+decode path running against a live RPC. `ChannelOpened` and
//!      `ChannelSettled` decode are asserted directly (via the resulting store
//!      transitions — persist then forget). `ChannelCloseInitiated` is
//!      observe-only in the watcher (no store side-effect), so the test
//!      exercises it but verifies only the resulting on-chain `Closing` status,
//!      not the decode itself.
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
//!    un-redeemed); graceful-shutdown `closeChannel` fires (assert the on-chain
//!    `Closing` status); after the dispute window `settleChannel` → assert the
//!    watcher decodes `ChannelSettled` and `forget`s the row from the store.
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
    // The single end-to-end flow is intentionally one long sequential test;
    // splitting it would fragment the shared anvil/deploy setup. Second-scale
    // timeouts read more clearly as `from_secs` than `from_mins` here.
    clippy::too_many_lines,
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
use decdn_incentive::payment_channel::PaymentChannel;
use decdn_incentive::{
    ChannelStateStore, bind_node_id_domain, binding_signing_hash, slash_judge_domain,
    voucher_domain,
};
use decdn_node::channel_store::PersistentChannelStateStore;
use decdn_node::client_requester::{ChannelContext, stream_fetch};
use decdn_node::metrics::Metrics;
use decdn_node::payment_settlement::PaymentChannelService;
use decdn_protocol::ALPN_CLIENT;
use iroh::EndpointAddr;

mod support;
use support::{
    HandlerDomains, build_handler_full, cache_with_blob, fresh_key, local_endpoint,
    permissive_limiter, spawn_server,
};

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
const DEPOSIT_MICRO_USDC: u64 = 10_000_000; // 10 USDC, ≥ contract minDeposit (1 USDC)

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
        function stake(uint256 amount) external;
        function registerNode(
            bytes32 nodeId,
            bytes multiaddrs,
            string regionHint,
            bytes bindingSignature,
            bytes ed25519Signature
        ) external;
        function isActive(address operator) external view returns (bool);
    }

    #[sol(rpc)]
    contract PaymentChannelOpen {
        function openChannel(address provider, uint256 deposit) external returns (bytes32 channelId);
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
    // Surface the redeemer/watcher background-task logs (the `warn!` carrying an
    // on-chain revert reason is the key diagnostic when a `withdraw`/`close`
    // poll times out). `try_init` is idempotent so a shared-process re-run is
    // harmless; `with_test_writer` routes through libtest's capture.
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();

    let contracts = contracts_dir();

    // ---- 0. Build contracts so artifacts + the deploy script are available.
    let build = Command::new("forge")
        .current_dir(&contracts)
        .args(["build"])
        .output()
        .expect("run `forge build` (is foundry installed?)");
    assert!(
        build.status.success(),
        "forge build failed:\n{}",
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
    run_deploy_script(&contracts, &rpc_url, usdc_addr, node_addr)?;
    let (capacity_bond, payment_channel, fee_router, token, slash_judge) =
        read_manifest(&manifest)?;

    // Build the per-role providers + contract handles.
    let node_provider = ProviderBuilder::new()
        .wallet(EthereumWallet::from(node_signer.clone()))
        .connect_http(url.clone());
    let client_provider = ProviderBuilder::new()
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

    // ---- 4. Activate the node operator: stake + registerNode → isActive.
    let min_stake: U256 = "50000000000000000000000".parse()?; // 50_000e18 (deploy default)
    token_erc20
        .approve(capacity_bond, min_stake)
        .send()
        .await?
        .get_receipt()
        .await?;
    bond.stake(min_stake).send().await?.get_receipt().await?;

    // Binding signature: eth key over EIP-712 BindNodeId(nodeId, nonce=0).
    let bind_domain = bind_node_id_domain(CHAIN_ID, capacity_bond);
    let binding_sig = node_signer
        .sign_hash_sync(&binding_signing_hash(node_id, 0, &bind_domain))?
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
    let store: Arc<dyn ChannelStateStore> =
        Arc::new(PersistentChannelStateStore::open(store_tmp.path())?);

    let node_eth = Arc::new(node_signer.clone());
    let metrics = Arc::new(Metrics::new());
    let domains = HandlerDomains {
        slash: slash_judge_domain(CHAIN_ID, slash_judge),
        voucher: voucher_domain(CHAIN_ID, payment_channel),
        binding: bind_domain.clone(),
    };
    let handler = build_handler_full(
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
    )?;

    let service = PaymentChannelService::bootstrap(
        node_provider.clone(),
        payment_channel,
        node_addr,
        Arc::clone(&store),
        Arc::clone(&handler),
        U256::from(REDEEM_THRESHOLD_MICRO_USDC),
    )
    .await?;
    handler.attach_redeem_hint(service.redeem_hint_sender());

    let (server_ep, server_addr) = local_endpoint(node_iroh_sk, vec![ALPN_CLIENT.to_vec()]).await?;
    let server_task = spawn_server(server_ep.clone(), Arc::clone(&handler));
    let (client_ep, _) = local_endpoint(fresh_key(), vec![]).await?;
    let target = EndpointAddr::new(node_pub).with_ip_addr(server_addr);

    // Give the watcher a moment to install its event filters before the first
    // ChannelOpened is emitted (filters only capture logs after creation).
    tokio::time::sleep(Duration::from_secs(2)).await;

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
        .openChannel(node_addr, deposit)
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
    // `expiresAt` is decoded from the event and feeds the expiry-sweep close
    // path; a non-zero value confirms the field (not just the id) round-tripped.
    anyhow::ensure!(
        persisted.expires_at != 0,
        "persisted channel 1 has no expiry — ChannelOpened.expiresAt was not decoded"
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
        .openChannel(node_addr, deposit)
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

    // Graceful shutdown closes the un-redeemed channel (closeChannel fires →
    // dispute window opens). Verifies the shutdown close path + that the
    // persisted voucher signature is accepted on-chain.
    service
        .close_open_channels_on_shutdown(Duration::from_secs(30))
        .await;
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
        "closeChannel did not move channel 2 to Closing"
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
fn run_deploy_script(
    contracts: &Path,
    rpc_url: &str,
    usdc: Address,
    initial_token_holder: Address,
) -> anyhow::Result<()> {
    let out = Command::new("forge")
        .current_dir(contracts)
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
        .env("CHALLENGER_INCENTIVE_POOL", DEPLOYER_ADDR)
        .env("FORCE_OVERWRITE_MANIFEST", "true")
        .output()
        .expect("run forge script");
    anyhow::ensure!(
        out.status.success(),
        "forge script DeployProtocol failed:\n{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(())
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
