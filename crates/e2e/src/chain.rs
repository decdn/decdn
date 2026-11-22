//! Chain fixture: spin up `anvil`, load a pre-deployed protocol state snapshot,
//! and expose typed `alloy` handles + the helper verbs (fund gas, mint
//! USDC/TOKEN, onboard an operator, read served bytes) the cross-layer journeys
//! need.
//!
//! The `DeployProtocol` forge script runs **once per test run**, not once per
//! journey. The first fixture to launch wins an advisory file lock, deploys the
//! full protocol into a throwaway anvil, and dumps the resulting chain state to
//! a cache file (see `ensure_shared_deployment`); every other fixture blocks
//! on the lock, then boots its own isolated anvil and replays that snapshot with
//! `anvil_loadState`. Each journey still gets a private chain — the win is that
//! the slow, contention-sensitive deploy leaves the per-journey hot path.
//!
//! Generalizes the self-contained bring-up in
//! `crates/node/tests/anvil_settlement_e2e.rs` into a reusable fixture. Like
//! that test it shells out to `forge`/`anvil` (no extra crate deps beyond the
//! advisory-lock helper) and so requires both on `PATH`; the fixture is only
//! reached behind the harness's `anvil-e2e` gate.

use std::fs::File;
use std::io::BufRead as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use alloy::network::{EthereumWallet, TransactionBuilder};
use alloy::primitives::{Address, B256, Bytes, U256, keccak256};
use alloy::providers::{DynProvider, Provider, ProviderBuilder};
use alloy::rpc::types::TransactionRequest;
use alloy::signers::SignerSync;
use alloy::signers::local::PrivateKeySigner;
use alloy::sol_types::SolValue;
use anyhow::Context;
use decdn_incentive::{
    binding_signing_hash, capacity_bond_domain, node_register, register_node_signing_hash,
};
use decdn_node::chain_events::snapshot::SNAPSHOT_LAG_MARGIN_BLOCKS;

use crate::bindings::{
    AccessControl, CapacityBond, ContentBlacklist, ContentBlacklistOrigin, Erc20,
    ManualVettingPolicy, OriginAssignment, PaymentPoolParams, PublisherRegistry,
};

/// The single chain id every anvil-e2e chain runs on: the one-time protocol
/// deploy and every per-journey fixture that loads its state snapshot.
///
/// A fixed value is what makes the state snapshot reusable. EIP-712 domain
/// separators bind to `block.chainid`, so the fixture that loads the snapshot
/// must run the same chain id the snapshot was deployed under, or every signed
/// voucher / node-id bind / stream signature the journeys build would verify
/// against a different domain. It is also the chain id every Rust-side signer
/// derives its domain from (see [`ChainFixture::chain_id`]).
///
/// The value sits in the test-only band that `contracts/.gitignore` masks
/// (`deployments/31337[67]*.json`), so the `deployments/31337690000.json`
/// manifest the one-time deploy writes stays untracked, and it is deliberately
/// distinct from `31337` (the local `dev-deploy.sh` manifest) so an e2e run and
/// a dev deploy never clobber each other's manifest.
const E2E_CHAIN_ID: u64 = 31_337_690_000;

/// Anvil dev account #0 — funded at genesis, broadcasts the deploy script.
const DEPLOYER_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
const DEPLOYER_ADDR: &str = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266";
/// Anvil dev account #1 — the "admin" EOA: deploys the mock USDC, holds the
/// initial TOKEN supply, and mints/transfers. Its address is always derived from
/// this key via [`admin_address`], never a parallel literal, so the EOA the
/// fixture funds can't drift from the wallet its provider signs with.
/// Deliberately NOT account #0 (the forge-script broadcaster, whose nonce the
/// script advances by ~25 — sharing it desyncs alloy's cached nonce).
const ADMIN_KEY: &str = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d";

/// Address the fixture grants `EMERGENCY_MULTISIG_ROLE` to, purely so
/// `registerRegionalBody` has a role-holding comparison side for its
/// signer-disjointness check. An EOA, so the on-chain `getOwners()` probe finds
/// nothing enumerable and registration takes ADR 011's off-chain-attestation
/// path — the realistic bootstrap posture, where no multisig is constituted yet.
const E2E_EMERGENCY_MULTISIG: Address = Address::new([0xE0; 20]);

/// Deploy default `minBond` (50_000e18 TOKEN).
const MIN_BOND_WEI: &str = "50000000000000000000000";

/// `FeeRouter`/`CapacityBond` epoch length, for the served-bytes read.
const EPOCH_LENGTH_SECS: u64 = 7 * 24 * 60 * 60;

// A cold compile of the full contracts suite can be slow; a single 180s cap with
// no retry made an over-budget-but-progressing build a hard failure. Give it
// headroom plus a bounded retry for a transient stall (mirrors the deploy retry).
// A real compile error still fails fast — only a timeout is retried. The base
// budget is scaled up under CI via `ci_scaled` (see #1384).
//
// Under CI this ladder is effectively never reached: `.github/workflows/ci.yml`
// hoists `forge build` into a one-time job step, so `contracts/out` is already
// warm and the in-test build is a sub-second incremental no-op. The cold cost is
// paid once per job, outside any per-test `OVERALL_TIMEOUT`, which is what lets
// the per-test ceilings shrink to the tiers in `crate::timeout` (see #1620). The
// ladder still guards local runs, which have no such job step.
const FORGE_BUILD_TIMEOUT: Duration = Duration::from_secs(300);
const BUILD_ATTEMPTS: usize = 3;
// The deploy runs once per test run, serialized by the bootstrap lock, so no
// two forge scripts ever broadcast at the same time — the `--test-threads 2`
// CPU starvation that made a per-journey deploy flaky (#1384) cannot occur here.
// `run_deploy_script` still reverts anvil to a pre-deploy snapshot between its
// own retry attempts (#785), and the budget is still scaled up under `ci_scaled`
// for a slow shared runner, but the base budget is short: one uncontended deploy
// is comfortably fast.
//
// The deploy ladder is still the binding in-test retry ladder the per-journey
// ceilings must contain (see `crate::timeout` and #1620): whichever journeys
// race the first `launch()` either run the deploy (the lock winner) or block on
// the lock for its duration, and both waits sit inside that journey's
// `OVERALL_TIMEOUT`. Two attempts cap the CI worst case at
// `2 * ci_scaled(60s) = 240s`, which fits under the 300s standard tier.
const DEPLOY_TIMEOUT: Duration = Duration::from_secs(60);
const DEPLOY_ATTEMPTS: usize = 2;
// The anvil-e2e deploy flakiness (#1384) is starvation, not slowness: under
// `--test-threads 2` on the 4-core CI runner (see `.github/workflows/ci.yml`),
// each journey spawns its own anvil + `forge script` (and often a `decdn-node`),
// so a forge budget that is ample on an idle dev box can still time out
// mid-progress under that contention. Scale the forge wall-clock budgets up when
// running under CI, leaving local runs at the tighter budget so a genuine local
// hang still fails fast.
const CI_TIMEOUT_MULTIPLIER: u32 = 2;
/// Linear backoff between deploy retries (`n * attempt`), giving a transient
/// runner-contention spike time to clear before the next forge process spawns.
const DEPLOY_RETRY_BACKOFF: Duration = Duration::from_secs(3);
/// How many times to drop anvil's pool while waiting for it to stay empty, and
/// how long to let stragglers land between drops. `kill_on_drop` SIGKILLs the
/// stalled forge but cannot unsend the transactions it already wrote to the
/// socket, so the pool has to be observed quiet, not merely dropped once.
const POOL_DRAIN_POLLS: usize = 5;
const POOL_DRAIN_SETTLE: Duration = Duration::from_millis(250);
/// How long anvil gets to print the address it listens on, and then to answer
/// its first RPC.
const ANVIL_START_TIMEOUT: Duration = Duration::from_secs(20);
/// The stdout line in which anvil reports its listen address.
const ANVIL_LISTENING: &str = "Listening on ";

/// Deployed protocol contract addresses, read from the forge-script manifest.
#[derive(Debug, Clone, Copy)]
pub struct ContractAddrs {
    /// `CapacityBond` — operator stake and the node registry.
    pub capacity_bond: Address,
    /// `PaymentPool` — the shared USDC pools clients fund and sellers redeem.
    pub payment_pool: Address,
    /// `FeeRouter` — splits redeemed revenue per ADR 026 § FeeRouter split.
    pub fee_router: Address,
    /// `Token` — the TOKEN ERC-20 used for staking and governance.
    pub token: Address,
    /// `DecdnGovernor` — the on-chain governor that proposes through the
    /// timelock below.
    pub governor: Address,
    /// `TimelockController` — holds `GOVERNANCE_ROLE` on the governed contracts
    /// after the `DeployProtocol` handoff, which renounces `DEFAULT_ADMIN_ROLE`
    /// to no one (#2028). The blacklist
    /// journeys drive governance actions by impersonating it (anvil), since the
    /// deployer keeps no privileged roles.
    pub timelock: Address,
    /// `PublisherRegistry` — namespace ownership and its timelocked transfer
    /// (ADR 002). Publisher identity is implicit on first `createNamespace`;
    /// vetting lives in the policy at `manual_vetting_policy` below.
    pub publisher_registry: Address,
    /// `OriginAssignment` — which bonded operators a publisher has seated as
    /// authorized origins for each of its namespaces (ADR 011).
    pub origin_assignment: Address,
    /// `ManualVettingPolicy` — the genesis vetting policy `OriginAssignment`
    /// reads. The fixture vets publishers here (see [`ChainFixture::vet_publisher`]).
    pub manual_vetting_policy: Address,
    /// `ContentBlacklist` — the on-chain takedown set the deny gate reads.
    pub content_blacklist: Address,
    /// `CctpSweepReceiver` — receives each `PaymentPool` sweep and deposits it
    /// into `FeeRouter` (see [`ChainFixture::sweep`]).
    pub cctp_sweep_receiver: Address,
}

/// Kills the spawned `anvil` on drop — a panicking assertion never leaks the
/// process. `manifests` is non-empty only for the throwaway anvil the one-time
/// deploy runs in: dropping it removes the (gitignored) `deployments/*.json`
/// files the forge scripts wrote. Per-journey anvils load a state snapshot
/// instead of deploying, so they write no manifest and carry none.
#[derive(Debug)]
struct AnvilGuard {
    child: Child,
    manifests: Vec<PathBuf>,
}

impl Drop for AnvilGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        for manifest in &self.manifests {
            let _ = std::fs::remove_file(manifest);
        }
    }
}

/// A live anvil process at [`E2E_CHAIN_ID`] with the handles a caller needs:
/// the drop guard, the endpoint (both string and parsed forms), and an
/// admin-signed provider.
struct AnvilProcess {
    guard: AnvilGuard,
    rpc_url: String,
    url: reqwest::Url,
    admin: DynProvider,
}

/// Bring up an `anvil` at [`E2E_CHAIN_ID`] on a port it picks itself (see
/// [`spawn_anvil_child`]) and wait for its RPC.
///
/// `manifests` are the `deployments/*.json` paths whose cleanup the guard owns:
/// the governance and payment manifests for the throwaway anvil the one-time
/// deploy writes into, none for a per-journey anvil that loads a snapshot.
async fn spawn_anvil(manifests: Vec<PathBuf>) -> anyhow::Result<AnvilProcess> {
    let admin_signer: PrivateKeySigner = ADMIN_KEY.parse().context("parse admin key")?;
    // Zero slots per epoch makes anvil's `safe` block the head, so a node's
    // `ContentBlacklist` ledger read (pinned to `safe`) sees a mutation as soon
    // as it mines.
    let (child, port) = spawn_anvil_child(&[
        "--chain-id",
        &E2E_CHAIN_ID.to_string(),
        "--slots-in-an-epoch",
        "0",
    ])
    .await?;
    let mut guard = AnvilGuard { child, manifests };
    let rpc_url = format!("http://127.0.0.1:{port}");

    // Every provider this fixture hands out builds a reqwest client.
    decdn_common::tls::install_crypto_provider();
    let url: reqwest::Url = rpc_url.parse().context("parse anvil rpc url")?;
    let admin: DynProvider = ProviderBuilder::new()
        .with_simple_nonce_management()
        .wallet(EthereumWallet::from(admin_signer))
        .connect_http(url.clone())
        .erased();

    let deadline = tokio::time::Instant::now() + ANVIL_START_TIMEOUT;
    while admin.get_chain_id().await.is_err() {
        if let Ok(Some(status)) = guard.child.try_wait() {
            anyhow::bail!("anvil exited before its RPC came up: {status}");
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "anvil RPC never came up within {ANVIL_START_TIMEOUT:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok(AnvilProcess {
        guard,
        rpc_url,
        url,
        admin,
    })
}

/// Spawn `anvil --port 0` with `args`, and return it with the loopback port it
/// bound.
///
/// Port 0 makes the kernel pick the port at anvil's own bind, so no other
/// process can take it first. anvil reports the port on stdout as
/// `Listening on 127.0.0.1:<port>`, which `--silent` suppresses, so `args` must
/// not hold `--silent`. A thread reads stdout up to that line, then discards the
/// rest (one line per RPC request) until anvil exits, so anvil never blocks on
/// a full pipe. The child is killed and reaped on every error.
async fn spawn_anvil_child(args: &[&str]) -> anyhow::Result<(Child, u16)> {
    let mut child = Command::new("anvil")
        .args(["--port", "0"])
        .args(args)
        .stdout(Stdio::piped())
        .spawn()
        .context("spawn anvil (is foundry installed?)")?;
    let port = match read_anvil_port(&mut child).await {
        Ok(port) => port,
        Err(err) => {
            let _ = child.kill();
            let _ = child.wait();
            return Err(err);
        }
    };
    Ok((child, port))
}

/// The port `child` (an anvil spawned with piped stdout) reports it listens on.
async fn read_anvil_port(child: &mut Child) -> anyhow::Result<u16> {
    let stdout = child
        .stdout
        .take()
        .context("anvil spawned without piped stdout")?;
    let (tx, rx) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name("anvil-stdout".to_owned())
        .spawn(move || {
            let mut reader = std::io::BufReader::new(stdout);
            let mut line = Vec::new();
            loop {
                line.clear();
                match reader.read_until(b'\n', &mut line) {
                    Ok(0) | Err(_) => return,
                    Ok(_) => {
                        if let Some(port) = anvil_listen_port(&String::from_utf8_lossy(&line)) {
                            let _ = tx.send(port);
                            break;
                        }
                    }
                }
            }
            let _ = std::io::copy(&mut reader, &mut std::io::sink());
        })
        .context("spawn the anvil stdout reader")?;
    match tokio::time::timeout(ANVIL_START_TIMEOUT, rx).await {
        Ok(Ok(port)) => Ok(port),
        Ok(Err(_)) => anyhow::bail!(
            "anvil closed stdout before it printed `{ANVIL_LISTENING}<addr>` (exit: {:?})",
            child.try_wait().ok().flatten()
        ),
        Err(_) => anyhow::bail!(
            "anvil printed no `{ANVIL_LISTENING}<addr>` within {ANVIL_START_TIMEOUT:?}"
        ),
    }
}

/// The port in anvil's `Listening on <addr>` stdout line, or `None` for any
/// other line.
fn anvil_listen_port(line: &str) -> Option<u16> {
    line.trim()
        .strip_prefix(ANVIL_LISTENING)?
        .parse::<std::net::SocketAddr>()
        .ok()
        .map(|addr| addr.port())
}

/// A live anvil deployment of the full protocol with typed handles.
///
/// The descriptive handles (endpoint, chain id, addresses, admin provider) are
/// facts about the already-running anvil process, fixed at [`Self::launch`].
/// They are exposed as accessors rather than `pub` fields so a caller can't
/// reassign one and silently desync the fixture from the live process it
/// describes (the endpoint is one stored form that derives both the RPC URL
/// and the URL, so they cannot disagree).
#[derive(Debug)]
pub struct ChainFixture {
    _anvil: AnvilGuard,
    /// Parsed RPC endpoint — the single stored form; `rpc_url()` derives the
    /// string.
    url: reqwest::Url,
    chain_id: u64,
    addrs: ContractAddrs,
    usdc: Address,
    manifest_contracts: Vec<(String, Address)>,
    admin: DynProvider,
    admin_addr: Address,
}

impl ChainFixture {
    /// Spawn an isolated anvil and replay the shared post-deploy state snapshot
    /// into it, returning the fixture with typed handles. The first caller in a
    /// test run also builds contracts and runs the one-time protocol deploy that
    /// produces the snapshot (see `ensure_shared_deployment`). Requires `anvil`
    /// + `forge` on `PATH`.
    pub async fn launch() -> anyhow::Result<Self> {
        let contracts = contracts_dir()?;

        // Deploy once per test run (behind a cross-process lock); every journey
        // replays the resulting state snapshot into its own isolated anvil.
        let shared = ensure_shared_deployment(&contracts).await?;

        let anvil = spawn_anvil(Vec::new()).await?;
        load_state_snapshot(&anvil.admin, &shared.state_path).await?;
        // A node pins its enumerations `SNAPSHOT_LAG_MARGIN_BLOCKS` below head.
        // Anvil keeps no state at or below the loaded head and answers a read
        // there with `BlockOutOfRangeError`, not empty code, so the node's
        // code-presence fallback cannot catch it. Mine past the margin so every
        // later snapshot pins a block that holds the deployment.
        crate::time::mine_blocks(&anvil.admin, SNAPSHOT_LAG_MARGIN_BLOCKS + 1)
            .await
            .context("mine past the boot snapshot lag margin")?;

        let admin_addr = admin_address()?;
        Ok(Self {
            _anvil: anvil.guard,
            url: anvil.url,
            chain_id: E2E_CHAIN_ID,
            addrs: shared.manifest.addrs,
            usdc: shared.manifest.usdc,
            manifest_contracts: shared.manifest.contracts,
            admin: anvil.admin,
            admin_addr,
        })
    }

    /// JSON-RPC endpoint string (e.g. `http://127.0.0.1:PORT`).
    #[must_use]
    pub fn rpc_url(&self) -> String {
        // `Url::as_str` appends a trailing slash for the empty path; trim it
        // back to the exact `http://host:port` form the fixture built.
        self.url.as_str().trim_end_matches('/').to_string()
    }

    /// Per-fixture chain id (anvil `--chain-id`); also the EIP-712 / ed25519
    /// domain chain id every signature in this fixture is bound to.
    #[must_use]
    pub const fn chain_id(&self) -> u64 {
        self.chain_id
    }

    /// Deployed protocol contract addresses.
    #[must_use]
    pub const fn addrs(&self) -> ContractAddrs {
        self.addrs
    }

    /// The fixture's `PaymentPool` deployment: this chain and its pool.
    #[must_use]
    pub const fn deployment(&self) -> decdn_incentive::Deployment {
        decdn_incentive::Deployment {
            chain_id: self.chain_id,
            payment_pool: self.addrs.payment_pool,
        }
    }

    /// Mock USDC (mintable) the settlement token points at.
    #[must_use]
    pub const fn usdc(&self) -> Address {
        self.usdc
    }

    /// Every entry of the deploy manifest's `contracts` map, as `(name,
    /// address)` pairs sorted by name. Unlike [`Self::addrs`], this lists a
    /// contract the fixture has no typed field for, so a journey that must
    /// cover the whole deployment picks up a new contract without an edit. An
    /// entry the deploy left undeployed carries the zero address.
    #[must_use]
    pub fn manifest_contracts(&self) -> &[(String, Address)] {
        &self.manifest_contracts
    }

    /// Admin provider (anvil dev #1): raw RPC, minting, TOKEN distribution.
    #[must_use]
    pub const fn admin(&self) -> &DynProvider {
        &self.admin
    }

    /// The admin EOA address (initial TOKEN holder, mock-USDC minter).
    #[must_use]
    pub const fn admin_addr(&self) -> Address {
        self.admin_addr
    }

    /// Deploy a second `PaymentPool` beside the snapshot's — the chain an
    /// operator sees after a `PaymentPool` redeploy, or a second pool contract
    /// on the same payment chain — and return its address.
    ///
    /// The new pool copies every constructor parameter of the snapshot pool:
    /// its USDC, its CCTP messenger and sweep recipient, its dispute window,
    /// operator share, minimum sweep, minimum deposit and pauser. A node that
    /// accepts it serves and redeems against it as against the snapshot pool.
    ///
    /// `ownerPoolNonce` starts at zero on the new pool, so every owner's Nth pool
    /// here has the same id as its Nth pool on [`ContractAddrs::payment_pool`].
    pub async fn redeploy_payment_pool(&self) -> anyhow::Result<Address> {
        let contracts = contracts_dir()?;
        let snapshot = PaymentPoolParams::new(self.addrs.payment_pool, &self.admin);
        let read = |what: &str| format!("snapshot PaymentPool.{what}");
        // PaymentPool(usdc, tokenMessenger, sweepRecipient, sweepDomain,
        //             disputeWindow, operatorBps, minSweep, minDeposit, pauser)
        let pool_args = (
            snapshot.usdc().call().await.context(read("usdc"))?,
            snapshot
                .tokenMessenger()
                .call()
                .await
                .context(read("tokenMessenger"))?,
            snapshot
                .sweepRecipient()
                .call()
                .await
                .context(read("sweepRecipient"))?,
            snapshot
                .sweepDomain()
                .call()
                .await
                .context(read("sweepDomain"))?,
            snapshot
                .disputeWindow()
                .call()
                .await
                .context(read("disputeWindow"))?,
            snapshot
                .operatorBps()
                .call()
                .await
                .context(read("operatorBps"))?,
            snapshot.minSweep().call().await.context(read("minSweep"))?,
            snapshot
                .minDeposit()
                .call()
                .await
                .context(read("minDeposit"))?,
            snapshot.pauser().call().await.context(read("pauser"))?,
        );
        anyhow::ensure!(
            pool_args.0 == self.usdc,
            "the snapshot PaymentPool settles in {} (want {})",
            pool_args.0,
            self.usdc
        );
        let pool = deploy_artifact(
            &self.admin,
            &contracts,
            "PaymentPool",
            &pool_args.abi_encode_params(),
        )
        .await?;
        Ok(pool)
    }

    /// Build a wallet-filled provider for `signer` (simple nonce management
    /// and the send-leg marker mirror production — a reverted gas-estimate
    /// must not leak a cached nonce and gap the lane, and the client senders
    /// read the marker to tell a lost send from a failed chain read).
    #[must_use]
    pub fn provider_for(&self, signer: &PrivateKeySigner) -> DynProvider {
        ProviderBuilder::new()
            .layer(decdn_incentive::tx::SendLegLayer)
            .with_simple_nonce_management()
            .wallet(EthereumWallet::from(signer.clone()))
            .connect_http(self.url.clone())
            .erased()
    }

    /// Fund `who` with `eth` whole-ETH of gas via `anvil_setBalance`.
    pub async fn fund_eth(&self, who: Address, eth: u64) -> anyhow::Result<()> {
        let wei = U256::from(eth) * U256::from(10u64).pow(U256::from(18));
        let _: serde_json::Value = self
            .admin
            .raw_request("anvil_setBalance".into(), (who, wei))
            .await
            .context("anvil_setBalance")?;
        Ok(())
    }

    /// Mint `amount` mock-USDC base units to `to` (admin is the minter).
    pub async fn mint_usdc(&self, to: Address, amount: U256) -> anyhow::Result<()> {
        let receipt = Erc20::new(self.usdc, &self.admin)
            .mint(to, amount)
            .send()
            .await
            .context("usdc.mint send")?
            .get_receipt()
            .await
            .context("usdc.mint receipt")?;
        crate::ensure_mined(&receipt, "usdc.mint")
    }

    /// Transfer `amount` TOKEN from the admin (initial holder) to `to`.
    pub async fn transfer_token(&self, to: Address, amount: U256) -> anyhow::Result<()> {
        let receipt = Erc20::new(self.addrs.token, &self.admin)
            .transfer(to, amount)
            .send()
            .await
            .context("token.transfer send")?
            .get_receipt()
            .await
            .context("token.transfer receipt")?;
        crate::ensure_mined(&receipt, "token.transfer")
    }

    /// Take an operator from bare to on-chain `isActive`: fund gas, stake the
    /// minimum bond (TOKEN transferred from admin → approve → `bond`), then
    /// `registerNode` with a real EIP-712 binding signature (operator eth key)
    /// and ed25519 ownership proof (`node_secret`, the iroh node key) over the
    /// production digest the `Ed25519Verifier` checks.
    pub async fn onboard_operator(
        &self,
        operator: &PrivateKeySigner,
        node_secret: &iroh::SecretKey,
        region: &str,
        multiaddr: &str,
    ) -> anyhow::Result<()> {
        let op_addr = operator.address();
        let min_bond: U256 = MIN_BOND_WEI.parse().context("parse min bond")?;

        self.fund_eth(op_addr, 100).await?;
        self.transfer_token(op_addr, min_bond).await?;

        let op_provider = self.provider_for(operator);
        let bond = CapacityBond::new(self.addrs.capacity_bond, &op_provider);
        let approve_receipt = Erc20::new(self.addrs.token, &op_provider)
            .approve(self.addrs.capacity_bond, min_bond)
            .send()
            .await
            .context("token.approve send")?
            .get_receipt()
            .await
            .context("token.approve receipt")?;
        crate::ensure_mined(&approve_receipt, "token.approve")?;
        let bond_receipt = bond
            .bond(min_bond)
            .send()
            .await
            .context("bond send")?
            .get_receipt()
            .await
            .context("bond receipt")?;
        crate::ensure_mined(&bond_receipt, "bond")?;

        self.register_node_raw(operator, node_secret, region, multiaddr)
            .await?;

        anyhow::ensure!(
            bond.isActive(op_addr)
                .call()
                .await
                .context("read isActive")?,
            "operator must be active after bond + registerNode"
        );
        Ok(())
    }

    /// Fund `operator` with gas and exactly `amount` TOKEN, then `approve` +
    /// `bond` that whole amount. Returns once the bond is mined.
    ///
    /// The bond-only half of [`Self::onboard_operator`], for journeys that need
    /// an operator at a *chosen* bond rather than at `minBond` (#1030). The
    /// amount is deliberately not clamped: `bond()` itself has no floor — it
    /// guards only `ZeroAmount` — and the whole point of the under-bonded
    /// negative is that the floor is enforced downstream, at `registerNode`.
    pub async fn fund_and_bond(
        &self,
        operator: &PrivateKeySigner,
        amount: U256,
    ) -> anyhow::Result<()> {
        let op_addr = operator.address();
        self.fund_eth(op_addr, 100).await?;
        self.transfer_token(op_addr, amount).await?;

        let op_provider = self.provider_for(operator);
        let approve_receipt = Erc20::new(self.addrs.token, &op_provider)
            .approve(self.addrs.capacity_bond, amount)
            .send()
            .await
            .context("token.approve send")?
            .get_receipt()
            .await
            .context("token.approve receipt")?;
        crate::ensure_mined(&approve_receipt, "token.approve")?;

        let bond_receipt = CapacityBond::new(self.addrs.capacity_bond, &op_provider)
            .bond(amount)
            .send()
            .await
            .context("bond send")?
            .get_receipt()
            .await
            .context("bond receipt")?;
        crate::ensure_mined(&bond_receipt, "bond")
    }

    /// Submit `registerNode` for `operator` with fully valid signatures, and
    /// surface a revert instead of hiding it.
    ///
    /// The signing half of [`Self::onboard_operator`], split out so a journey
    /// can drive `registerNode` on its own terms: as an arbitrary signer, at an
    /// arbitrary bond, or against a node id someone else already owns (#1030).
    /// `onboard_operator` is built on it, so the two can never disagree about
    /// how a registration is signed.
    ///
    /// Funds nothing and bonds nothing — the caller sets up the on-chain
    /// preconditions it wants to test. That is the point: `registerNode`'s
    /// guards fire in a fixed order (`_checkRegistrationPreconditions` before
    /// `_checkBindingOneToOne`), so a negative that under-funds its impostor
    /// reverts on the bond floor and never reaches the binding rule it meant to
    /// exercise.
    ///
    /// **Signatures are always real.** The EIP-712 binding signature is made
    /// with `operator`'s eth key and the ed25519 ownership proof with
    /// `node_secret`, both over the production digests, both at freshly-read
    /// nonces. A negative built on this therefore proves the guard it names
    /// fired — not that a malformed signature was rejected first.
    ///
    /// The error is the `anyhow`-wrapped `alloy::contract::Error`, so
    /// [`crate::assert::expect_revert_anyhow`] can downcast it and match the
    /// revert selector.
    pub async fn register_node_raw(
        &self,
        operator: &PrivateKeySigner,
        node_secret: &iroh::SecretKey,
        region: &str,
        multiaddr: &str,
    ) -> anyhow::Result<()> {
        let op_addr = operator.address();
        let node_id = B256::from_slice(node_secret.public().as_bytes());
        let op_provider = self.provider_for(operator);
        let bond = CapacityBond::new(self.addrs.capacity_bond, &op_provider);

        // Nonces feed both signature digests (fresh operator/nodeId → 0, but
        // read them so a re-onboard converges rather than signing a stale nonce).
        let binding_nonce = bond
            .bindingNonce(op_addr)
            .call()
            .await
            .context("read bindingNonce")?;
        let registration_nonce = bond
            .registrationNonce(node_id)
            .call()
            .await
            .context("read registrationNonce")?;

        // ADR 019 § Terms Acceptance — the registration signature commits to
        // the on-chain `currentTermsHash`; read it back so this matches whatever
        // genesis terms the deploy committed.
        let terms_hash = bond
            .currentTermsHash()
            .call()
            .await
            .context("read currentTermsHash")?;
        let domain = capacity_bond_domain(self.chain_id, self.addrs.capacity_bond);
        let bind_hash = register_node_signing_hash(node_id, binding_nonce, terms_hash, &domain);
        let binding_sig = operator
            .sign_hash_sync(&bind_hash)
            .context("sign binding hash")?
            .as_bytes()
            .to_vec();

        let digest = node_register::ownership_message_digest(
            node_id,
            op_addr,
            self.chain_id,
            registration_nonce,
        );
        let ed_sig = node_secret.sign(digest.as_slice()).to_bytes().to_vec();
        let packed_multiaddrs =
            node_register::pack_multiaddrs(&[multiaddr.to_string()]).context("pack multiaddrs")?;

        let register_receipt = bond
            .registerNode(
                node_id,
                Bytes::from(packed_multiaddrs),
                region.to_string(),
                terms_hash,
                Bytes::from(binding_sig),
                Bytes::from(ed_sig),
            )
            .send()
            .await
            .context("registerNode send")?
            .get_receipt()
            .await
            .context("registerNode receipt")?;
        crate::ensure_mined(&register_receipt, "registerNode")
    }

    /// Fund a node operator as a paid **buyer** so its daemon can open an
    /// upstream `PaymentPool` and pay for a node-to-node cache-miss pull.
    ///
    /// [`Self::onboard_operator`] funds only the operator's *seller* role (a
    /// TOKEN capacity bond); it never mints the USDC a buyer pool escrows, so
    /// a node whose miss must pay an upstream peer would fail at pool open.
    /// This mints `usdc` settlement units to the operator address (the daemon
    /// opens buyer pools under the same eth key it bonds and settles with).
    ///
    /// It deliberately does **not** set the `PaymentPool` allowance: the
    /// daemon does that itself, once, at buyer bootstrap (`blockchain.buyer_max_approve`
    /// defaults to `true`, so `BuyerPoolService::bootstrap` sends the approve
    /// from the node's own wallet before pull-through is provisioned). Approving
    /// here from the same operator key would race the daemon's approve on that
    /// account's nonce, so minting is the fixture's whole job — the approval is
    /// the node's. `usdc` need only cover the buyer deposit
    /// (`blockchain.buyer_working_deposit_micro_usdc`, default 10 USDC) plus any
    /// recovery top-up back toward it; mint generously so a multi-interval pull
    /// never starves the pool.
    pub async fn fund_node_as_buyer(
        &self,
        operator_addr: Address,
        usdc: U256,
    ) -> anyhow::Result<()> {
        self.mint_usdc(operator_addr, usdc)
            .await
            .context("mint node-buyer USDC")
    }

    /// Rebind `operator` to `new_secret`'s node id via `CapacityBond.bindNodeId`
    /// — the iroh key-rotation path, driven directly rather than through the CLI
    /// (#1034, G-NODE-07).
    ///
    /// The signature ingredients mirror [`Self::onboard_operator`] with two
    /// differences that matter: the EIP-712 payload is the terms-free
    /// `BindNodeId` (rebinding does not re-accept terms), and the ed25519 proof
    /// is signed by the **new** key over `registrationNonce[newNodeId]` — not
    /// the old key, and not the old id's nonce.
    ///
    /// Exists alongside the CLI-driven journey so a test can set up an
    /// already-rotated chain state without paying for a CLI subprocess, and so a
    /// CLI regression cannot silently make the fixture agree with it.
    pub async fn rotate_node_id(
        &self,
        operator: &PrivateKeySigner,
        new_secret: &iroh::SecretKey,
    ) -> anyhow::Result<()> {
        let op_addr = operator.address();
        let new_node_id = B256::from_slice(new_secret.public().as_bytes());

        let op_provider = self.provider_for(operator);
        let bond = CapacityBond::new(self.addrs.capacity_bond, &op_provider);

        let binding_nonce = bond
            .bindingNonce(op_addr)
            .call()
            .await
            .context("read bindingNonce")?;
        // Keyed on the NEW id: a fresh key is usually 0, but an id that was
        // bound and unbound before carries a bumped nonce, and signing over 0
        // there reverts `InvalidEd25519Signature`.
        let registration_nonce = bond
            .registrationNonce(new_node_id)
            .call()
            .await
            .context("read registrationNonce")?;

        let domain = capacity_bond_domain(self.chain_id, self.addrs.capacity_bond);
        let bind_hash = binding_signing_hash(new_node_id, binding_nonce, &domain);
        let binding_sig = operator
            .sign_hash_sync(&bind_hash)
            .context("sign binding hash")?
            .as_bytes()
            .to_vec();

        let digest = node_register::ownership_message_digest(
            new_node_id,
            op_addr,
            self.chain_id,
            registration_nonce,
        );
        let ed_sig = new_secret.sign(digest.as_slice()).to_bytes().to_vec();

        let receipt = bond
            .bindNodeId(new_node_id, Bytes::from(binding_sig), Bytes::from(ed_sig))
            .send()
            .await
            .context("bindNodeId send")?
            .get_receipt()
            .await
            .context("bindNodeId receipt")?;
        crate::ensure_mined(&receipt, "bindNodeId")?;

        anyhow::ensure!(
            self.node_id_of(op_addr).await? == new_node_id,
            "operator must be bound to the new node id after bindNodeId"
        );
        Ok(())
    }

    /// The node id `operator` is bound to, or `B256::ZERO` when unbound.
    ///
    /// Reads `addressToNodeId` rather than `nodeIdOf`, deliberately: the raw
    /// binding is what a rotation swaps. `nodeIdOf` returns the same id but
    /// pairs it with a liveness flag that has nothing to do with the binding.
    pub async fn node_id_of(&self, operator: Address) -> anyhow::Result<B256> {
        CapacityBond::new(self.addrs.capacity_bond, &self.admin)
            .addressToNodeId(operator)
            .call()
            .await
            .context("read addressToNodeId")
    }

    /// The operator a node id is bound to, or `Address::ZERO` when the id is
    /// unbound. The inverse of [`Self::node_id_of`]: after a rotation the
    /// retired id reads back as `Address::ZERO`, which is what proves
    /// `bindNodeId` cleared the old mapping rather than merely adding a new one.
    pub async fn operator_of_node_id(&self, node_id: B256) -> anyhow::Result<Address> {
        CapacityBond::new(self.addrs.capacity_bond, &self.admin)
            .nodeIdToAddress(node_id)
            .call()
            .await
            .context("read nodeIdToAddress")
    }

    /// Unix timestamp of `operator`'s first bond — the ADR 026 age-ramp anchor.
    /// An iroh-key rotation must preserve it; an Ethereum-key migration cannot.
    pub async fn first_bonded_at(&self, operator: Address) -> anyhow::Result<u64> {
        CapacityBond::new(self.addrs.capacity_bond, &self.admin)
            .firstBondedAt(operator)
            .call()
            .await
            .context("read firstBondedAt")
    }

    /// Create a namespace owned by `owner` and return its id (parsed from the
    /// `createNamespace` return value). Drives the publisher control plane the
    /// origin journeys depend on (#1038/#1039).
    pub async fn create_namespace(&self, owner: &PrivateKeySigner) -> anyhow::Result<U256> {
        self.fund_eth(owner.address(), 10).await?;
        let provider = self.provider_for(owner);
        let registry = PublisherRegistry::new(self.addrs.publisher_registry, &provider);
        let id = registry
            .createNamespace()
            .call()
            .await
            .context("createNamespace call (static)")?;
        let receipt = registry
            .createNamespace()
            .send()
            .await
            .context("createNamespace send")?
            .get_receipt()
            .await
            .context("createNamespace receipt")?;
        // The static `call` above returns the id the `send` *would* mint; only
        // trust it once the real transaction is confirmed non-reverted.
        crate::ensure_mined(&receipt, "createNamespace")?;
        Ok(id)
    }

    /// Current authorized-origin addresses for `namespace` (namespace 0 has no
    /// authorized origins, so `getOrigins(0)` is always empty).
    pub async fn origins(&self, namespace: U256) -> anyhow::Result<Vec<Address>> {
        OriginAssignment::new(self.addrs.origin_assignment, &self.admin)
            .getOrigins(namespace)
            .call()
            .await
            .context("getOrigins")
    }

    /// Whether `operator` is authorized for `namespace` in chain truth.
    pub async fn is_authorized_origin(
        &self,
        namespace: U256,
        operator: Address,
    ) -> anyhow::Result<bool> {
        OriginAssignment::new(self.addrs.origin_assignment, &self.admin)
            .isAuthorizedOrigin(namespace, operator)
            .call()
            .await
            .context("isAuthorizedOrigin")
    }

    /// Seat one operator as an authorized origin for `namespace`, as its owner.
    /// The owner must already be a vetted publisher ([`Self::vet_publisher`]);
    /// seating takes effect immediately and emits `OriginAdded`.
    pub async fn add_origin(
        &self,
        owner: &PrivateKeySigner,
        namespace: U256,
        operator: Address,
    ) -> anyhow::Result<()> {
        let provider = self.provider_for(owner);
        let receipt = OriginAssignment::new(self.addrs.origin_assignment, &provider)
            .addOrigin(namespace, operator)
            .send()
            .await
            .context("addOrigin send")?
            .get_receipt()
            .await
            .context("addOrigin receipt")?;
        crate::ensure_mined(&receipt, "addOrigin")
    }

    /// Vet `publisher` through the genesis `ManualVettingPolicy`. Governance
    /// (the Timelock) admins `VETTER_ROLE`, so the fixture impersonates the
    /// Timelock to grant itself `VETTER_ROLE` (idempotent) and then calls
    /// `setVetted`. This mirrors how an operator with `VETTER_ROLE` vets a
    /// publisher on the live network, and takes effect instantly.
    pub async fn vet_publisher(&self, publisher: Address) -> anyhow::Result<()> {
        self.impersonate(self.addrs.timelock).await?;
        let raw = self.raw_provider();
        let policy = ManualVettingPolicy::new(self.addrs.manual_vetting_policy, &raw);

        let vetter_role = policy
            .VETTER_ROLE()
            .call()
            .await
            .context("read VETTER_ROLE")?;
        let grant = policy
            .grantRole(vetter_role, self.addrs.timelock)
            .from(self.addrs.timelock)
            .send()
            .await
            .context("grant VETTER_ROLE send")?
            .get_receipt()
            .await
            .context("grant VETTER_ROLE receipt")?;
        crate::ensure_mined(&grant, "grantRole VETTER_ROLE")?;

        let receipt = policy
            .setVetted(publisher, true)
            .from(self.addrs.timelock)
            .send()
            .await
            .context("setVetted send")?
            .get_receipt()
            .await
            .context("setVetted receipt")?;
        crate::ensure_mined(&receipt, "setVetted")
    }

    /// The bytes `operator` was paid for: the served bytes the fixture
    /// `PaymentPool` holds for its next sweep (`pending(operator)`), plus
    /// `FeeRouter.bytesPerEpoch(operator)` for the epoch at the chain's head
    /// timestamp, the governance-canonical counter a received sweep credits
    /// (ADR 036). A redemption raises the first at once.
    pub async fn served_bytes(&self, operator: Address) -> anyhow::Result<U256> {
        let block = self
            .admin
            .get_block(alloy::eips::BlockId::latest())
            .await
            .context("get latest block")?
            .ok_or_else(|| anyhow::anyhow!("no latest block"))?;
        let epoch = block.header.timestamp / EPOCH_LENGTH_SECS;
        let fee = crate::bindings::FeeRouter::new(self.addrs.fee_router, &self.admin);
        let swept = fee
            .bytesPerEpoch(operator, epoch)
            .call()
            .await
            .context("read bytesPerEpoch")?;
        let pending = crate::bindings::PaymentPool::new(self.addrs.payment_pool, &self.admin)
            .pending(operator)
            .call()
            .await
            .context("read PaymentPool.pending")?;
        Ok(swept.saturating_add(pending.servedBytes))
    }

    /// Carry the listed operators' pending protocol shares and served bytes
    /// to `FeeRouter` in one batch, as `decdn node sweep` and
    /// `decdn node receive-sweeps` do: `PaymentPool.sweep(operators)` burns
    /// through the loopback CCTP, and [`Self::receive_sweep`] delivers its
    /// message. The served bytes then count in `FeeRouter`'s epoch counter,
    /// which governance vote weight reads.
    pub async fn sweep(&self, operators: &[Address]) -> anyhow::Result<()> {
        let receipt = crate::bindings::PaymentPool::new(self.addrs.payment_pool, &self.admin)
            .sweep(operators.to_vec())
            .send()
            .await
            .context("PaymentPool.sweep send")?
            .get_receipt()
            .await
            .context("PaymentPool.sweep receipt")?;
        crate::ensure_mined(&receipt, "PaymentPool.sweep")?;
        self.receive_sweep(receipt.transaction_hash).await
    }

    /// Deliver the CCTP message of the sweep transaction `sweep_tx` with
    /// `CctpSweepReceiver.receiveSweep` (the loopback accepts any non-empty
    /// attestation), as `decdn node receive-sweeps` does after Circle
    /// attests it.
    pub async fn receive_sweep(&self, sweep_tx: B256) -> anyhow::Result<()> {
        use decdn_incentive::payment_pool::{CctpSweepReceiver, MessageTransmitterV2};
        let receipt = self
            .admin
            .get_transaction_receipt(sweep_tx)
            .await
            .context("the sweep receipt")?
            .with_context(|| format!("sweep {sweep_tx:#x} has no receipt"))?;
        let message = receipt
            .logs()
            .iter()
            .find_map(|log| log.log_decode::<MessageTransmitterV2::MessageSent>().ok())
            .map(|log| log.inner.data.message)
            .context("the sweep carries no CCTP MessageSent")?;
        let receipt = CctpSweepReceiver::new(self.addrs.cctp_sweep_receiver, &self.admin)
            .receiveSweep(message, Bytes::from_static(&[1]))
            .send()
            .await
            .context("CctpSweepReceiver.receiveSweep send")?
            .get_receipt()
            .await
            .context("CctpSweepReceiver.receiveSweep receipt")?;
        crate::ensure_mined(&receipt, "CctpSweepReceiver.receiveSweep")
    }

    /// Current chain head `block.timestamp` (seconds).
    pub async fn head_timestamp(&self) -> anyhow::Result<u64> {
        Ok(self
            .admin
            .get_block(alloy::eips::BlockId::latest())
            .await
            .context("get latest block")?
            .ok_or_else(|| anyhow::anyhow!("no latest block"))?
            .header
            .timestamp)
    }

    /// Anvil-impersonate `who` and fund it for gas, so `from = who` transactions
    /// are signed by anvil (used to act as the governance Timelock, which holds
    /// the privileged roles after the `DeployProtocol` handoff).
    async fn impersonate(&self, who: Address) -> anyhow::Result<()> {
        let _: serde_json::Value = self
            .admin
            .raw_request("anvil_impersonateAccount".into(), (who,))
            .await
            .context("anvil_impersonateAccount")?;
        self.fund_eth(who, 100).await
    }

    /// A wallet-less provider whose `eth_sendTransaction`s are signed by anvil
    /// for the request's `from` address (only valid while that account is
    /// impersonated).
    fn raw_provider(&self) -> DynProvider {
        ProviderBuilder::new()
            .connect_http(self.url.clone())
            .erased()
    }

    /// Add `hash` to the GLOBAL blacklist as governance. Impersonates the
    /// Timelock (which holds `GOVERNANCE_ROLE` after handoff) and blocks until
    /// mined. Emits `HashBlacklisted(GLOBAL, hash)`.
    pub async fn add_hash_global(&self, hash: B256) -> anyhow::Result<()> {
        self.impersonate(self.addrs.timelock).await?;
        let raw = self.raw_provider();
        let receipt = ContentBlacklist::new(self.addrs.content_blacklist, &raw)
            .addHashGlobal(hash, "e2e-takedown".to_string())
            .from(self.addrs.timelock)
            .send()
            .await
            .context("addHashGlobal send")?
            .get_receipt()
            .await
            .context("addHashGlobal receipt")?;
        crate::ensure_mined(&receipt, "addHashGlobal")
    }

    /// Remove `hash` from the GLOBAL blacklist as governance — the
    /// `removeHashGlobal` slow path that models an appeal / wrongful-entry
    /// reversal (ADR 011 § Content Takedown). Impersonates the Timelock (which
    /// holds `GOVERNANCE_ROLE` after handoff) and blocks until mined. Emits
    /// `HashRemoved(GLOBAL, hash)`, which the node's blacklist watcher projects by
    /// lifting the governance deny — the eviction itself stays sticky.
    pub async fn remove_hash_global(&self, hash: B256) -> anyhow::Result<()> {
        self.impersonate(self.addrs.timelock).await?;
        let raw = self.raw_provider();
        let receipt = ContentBlacklist::new(self.addrs.content_blacklist, &raw)
            .removeHashGlobal(hash)
            .from(self.addrs.timelock)
            .send()
            .await
            .context("removeHashGlobal send")?
            .get_receipt()
            .await
            .context("removeHashGlobal receipt")?;
        crate::ensure_mined(&receipt, "removeHashGlobal")
    }

    /// Blacklist (or un-blacklist) an origin operator as governance —
    /// `ContentBlacklist.setOriginBlacklist` (ADR 011 § Hash Evasion and Origin
    /// Blacklisting). Impersonates the Timelock (which holds `GOVERNANCE_ROLE`
    /// after handoff) and blocks until mined. Emits
    /// `OriginBlacklistUpdated(origin, blacklisted)`, which the node's blacklist
    /// watcher projects into its origin deny-set + announce-gate bar (#1398).
    pub async fn set_origin_blacklist(
        &self,
        origin: Address,
        blacklisted: bool,
    ) -> anyhow::Result<()> {
        self.impersonate(self.addrs.timelock).await?;
        let raw = self.raw_provider();
        let receipt = ContentBlacklistOrigin::new(self.addrs.content_blacklist, &raw)
            .setOriginBlacklist(origin, blacklisted)
            .from(self.addrs.timelock)
            .send()
            .await
            .context("setOriginBlacklist send")?
            .get_receipt()
            .await
            .context("setOriginBlacklist receipt")?;
        crate::ensure_mined(&receipt, "setOriginBlacklist")
    }

    /// Read the on-chain `isOriginBlacklisted(origin)` view — the fixture-side
    /// confirmation that [`Self::set_origin_blacklist`] landed, independent of
    /// the node's event-tail projection.
    pub async fn is_origin_blacklisted(&self, origin: Address) -> anyhow::Result<bool> {
        ContentBlacklistOrigin::new(self.addrs.content_blacklist, &self.admin)
            .isOriginBlacklisted(origin)
            .call()
            .await
            .context("isOriginBlacklisted")
    }

    /// `ContentBlacklist.addOperator` / `removeOperator` — the SECOND,
    /// independent address-level deny list (ADR 011 § Hash Evasion and Origin
    /// Blacklisting).
    ///
    /// Distinct from [`Self::set_origin_blacklist`] and not a convenience
    /// wrapper over it: `addOperator` writes `isOperatorBlacklisted` and emits
    /// `OperatorBlacklisted`, never touching `_isOriginBlacklisted` or
    /// `OriginBlacklistUpdated`. A consumer watching only the origin event
    /// misses this path entirely — and it is the primary governance route, since
    /// it also ejects the operator from `CapacityBond`. `OriginAssignment` treats
    /// the two as a union, so a node must too.
    pub async fn set_operator_blacklist(
        &self,
        operator: Address,
        blacklisted: bool,
    ) -> anyhow::Result<()> {
        self.impersonate(self.addrs.timelock).await?;
        let raw = self.raw_provider();
        let contract = ContentBlacklistOrigin::new(self.addrs.content_blacklist, &raw);
        // The two calls are distinct builder types, so each arm sends its own
        // rather than binding one variable.
        let (receipt, label) = if blacklisted {
            let r = contract
                .addOperator(operator)
                .from(self.addrs.timelock)
                .send()
                .await
                .context("addOperator send")?
                .get_receipt()
                .await
                .context("addOperator receipt")?;
            (r, "addOperator")
        } else {
            let r = contract
                .removeOperator(operator)
                .from(self.addrs.timelock)
                .send()
                .await
                .context("removeOperator send")?
                .get_receipt()
                .await
                .context("removeOperator receipt")?;
            (r, "removeOperator")
        };
        crate::ensure_mined(&receipt, label)
    }

    /// Read the on-chain `isOperatorBlacklisted(operator)` mapping — the
    /// fixture-side confirmation for [`Self::set_operator_blacklist`], and the
    /// other half of the union `OriginAssignment` evaluates.
    pub async fn is_operator_blacklisted(&self, operator: Address) -> anyhow::Result<bool> {
        ContentBlacklistOrigin::new(self.addrs.content_blacklist, &self.admin)
            .isOperatorBlacklisted(operator)
            .call()
            .await
            .context("isOperatorBlacklisted")
    }

    /// Add `hash` to `region`'s blacklist, acting as that region's registered
    /// body. `region` is the packed key from [`region_key`]. Emits
    /// `HashBlacklisted(region, hash)`.
    ///
    /// A bare `REGIONAL_BODY_ROLE` grant is NOT sufficient authority: ADR 011
    /// § Regional Governance Bodies scopes a body to exactly one jurisdiction,
    /// so `addHashRegional` also requires the caller to be the body registered
    /// for the region it names. This therefore registers a distinct
    /// region-derived body address (idempotently) and sends from it.
    pub async fn add_hash_regional(&self, region: B256, hash: B256) -> anyhow::Result<()> {
        let body = self.ensure_regional_body(region).await?;
        self.impersonate(body).await?;
        let raw = self.raw_provider();
        let receipt = ContentBlacklist::new(self.addrs.content_blacklist, &raw)
            .addHashRegional(region, hash, "e2e-takedown".to_string())
            .from(body)
            .send()
            .await
            .context("addHashRegional send")?
            .get_receipt()
            .await
            .context("addHashRegional receipt")?;
        crate::ensure_mined(&receipt, "addHashRegional")
    }

    /// Register (once) a body for `region` and return its address.
    ///
    /// The body address is derived from the region so each jurisdiction gets a
    /// distinct one — the contract enforces one-region-per-body in both
    /// directions, so reusing a single address across regions would revert on
    /// the second. Idempotent: a second call for the same region returns the
    /// already-registered body rather than re-registering.
    ///
    /// `registerRegionalBody` also takes the `EMERGENCY_MULTISIG_ROLE` holder to
    /// check signer-disjointness against, and verifies it actually holds the
    /// role, so the fixture grants that role to a fixed address first. Both are
    /// EOAs, so the on-chain `getOwners()` probe finds nothing enumerable and
    /// registration takes the ADR's off-chain-attestation path — which is the
    /// realistic bootstrap posture anyway.
    async fn ensure_regional_body(&self, region: B256) -> anyhow::Result<Address> {
        let blacklist = ContentBlacklist::new(self.addrs.content_blacklist, &self.admin);
        let existing = blacklist
            .getRegionalBody(region)
            .call()
            .await
            .context("getRegionalBody")?;
        if existing.body != Address::ZERO {
            return Ok(existing.body);
        }

        // Deterministic per-region body address; the low bytes of the region key
        // keep it distinct per jurisdiction and clear of the fixture's own
        // accounts.
        let mut raw = [0u8; 20];
        raw.copy_from_slice(&region.0[..20]);
        let body = Address::from(raw);

        self.impersonate(self.addrs.timelock).await?;
        let provider = self.raw_provider();
        let grant = AccessControl::new(self.addrs.content_blacklist, &provider)
            .grantRole(emergency_multisig_role(), E2E_EMERGENCY_MULTISIG)
            .from(self.addrs.timelock)
            .send()
            .await
            .context("grantRole EMERGENCY_MULTISIG_ROLE send")?
            .get_receipt()
            .await
            .context("grantRole receipt")?;
        crate::ensure_mined(&grant, "grantRole")?;

        let receipt = ContentBlacklist::new(self.addrs.content_blacklist, &provider)
            .registerRegionalBody(region, body, E2E_EMERGENCY_MULTISIG)
            .from(self.addrs.timelock)
            .send()
            .await
            .context("registerRegionalBody send")?
            .get_receipt()
            .await
            .context("registerRegionalBody receipt")?;
        crate::ensure_mined(&receipt, "registerRegionalBody")?;
        Ok(body)
    }

    /// Advance chain time by `secs` and mine, so a test can step past a
    /// contract-side window (e.g. the ADR 011 compliance window before a
    /// blacklist entry takes effect).
    pub async fn advance_time(&self, secs: u64) -> anyhow::Result<()> {
        let _: serde_json::Value = self
            .admin
            .raw_request("evm_increaseTime".into(), (secs,))
            .await
            .context("evm_increaseTime")?;
        let _: serde_json::Value = self
            .admin
            .raw_request("evm_mine".into(), ())
            .await
            .context("evm_mine")?;
        Ok(())
    }

    /// `ContentBlacklist.complianceWindow()` — the ADR 011 grace between an
    /// entry's `addedAt` and the moment it takes effect. Read rather than
    /// hardcoded so a governance change to the default cannot silently turn
    /// these tests into no-ops.
    pub async fn compliance_window(&self) -> anyhow::Result<u64> {
        ContentBlacklist::new(self.addrs.content_blacklist, &self.admin)
            .complianceWindow()
            .call()
            .await
            .context("read complianceWindow")
    }

    /// `CapacityBond.regionStabilityWindow()` — the ADR 030 region-change cooldown
    /// (and ripening window), in seconds. Read rather than hardcoded so a
    /// governance change to the default cannot silently turn a time-advance into
    /// a no-op.
    pub async fn region_stability_window(&self) -> anyhow::Result<u64> {
        let window = CapacityBond::new(self.addrs.capacity_bond, &self.admin)
            .regionStabilityWindow()
            .call()
            .await
            .context("read regionStabilityWindow")?;
        u64::try_from(window).context("regionStabilityWindow exceeds u64")
    }

    /// Change an operator's self-attested region via `CapacityBond.updateRegion`
    /// (ADR 030). Sent by the operator itself. The cooldown (=
    /// `REGION_STABILITY_WINDOW`) runs from when the current region took effect,
    /// including registration, so a caller must advance chain time past
    /// [`Self::region_stability_window`] before the change or the tx reverts
    /// `RegionCooldownActive`. Used to exercise a scope transition that emits no
    /// `ContentBlacklist` event.
    pub async fn update_region(
        &self,
        operator: &PrivateKeySigner,
        new_region: &str,
    ) -> anyhow::Result<()> {
        let provider = self.provider_for(operator);
        let receipt = CapacityBond::new(self.addrs.capacity_bond, &provider)
            .updateRegion(new_region.to_string())
            .send()
            .await
            .context("updateRegion send")?
            .get_receipt()
            .await
            .context("updateRegion receipt")?;
        crate::ensure_mined(&receipt, "updateRegion")
    }

    /// `CapacityBond.activeBond(operator)` (TOKEN base units).
    pub async fn active_bond(&self, operator: Address) -> anyhow::Result<U256> {
        CapacityBond::new(self.addrs.capacity_bond, &self.admin)
            .activeBond(operator)
            .call()
            .await
            .context("read activeBond")
    }

    /// `CapacityBond.declaredMbps(operator)` — the self-attested capacity tier.
    pub async fn declared_mbps(&self, operator: Address) -> anyhow::Result<u64> {
        let mbps = CapacityBond::new(self.addrs.capacity_bond, &self.admin)
            .declaredMbps(operator)
            .call()
            .await
            .context("read declaredMbps")?;
        Ok(u64::try_from(mbps).unwrap_or(u64::MAX))
    }

    /// `CapacityBond.bondRequired(mbps)` — the ADR 026 capacity-bond curve.
    /// Read rather than recomputed so a governance retune of `k`/`α` can't
    /// desynchronise a journey's expectations from the deployed curve.
    pub async fn bond_required(&self, mbps: u64) -> anyhow::Result<U256> {
        CapacityBond::new(self.addrs.capacity_bond, &self.admin)
            .bondRequired(U256::from(mbps))
            .call()
            .await
            .context("read bondRequired")
    }

    /// Emergency-multisig `CapacityBond.pause()`. The e2e deploy sets
    /// `EMERGENCY_MULTISIG = DEPLOYER_ADDR` (anvil dev #0), so the deployer key
    /// holds `PAUSER_ROLE`.
    ///
    /// Exists so a journey can make a *write* revert while every read still
    /// succeeds: `bond`/`declareMbps` are `whenNotPaused`, but the balance,
    /// allowance and curve views a CLI pre-flight uses are not. That is the only
    /// deterministic way to drive a command past its pre-flight gate and into a
    /// mid-sequence on-chain failure.
    pub async fn pause_capacity_bond(&self) -> anyhow::Result<()> {
        let deployer: PrivateKeySigner = DEPLOYER_KEY.parse().context("parse deployer key")?;
        let provider = self.provider_for(&deployer);
        let receipt = CapacityBond::new(self.addrs.capacity_bond, &provider)
            .pause()
            .send()
            .await
            .context("CapacityBond.pause send")?
            .get_receipt()
            .await
            .context("CapacityBond.pause receipt")?;
        crate::ensure_mined(&receipt, "CapacityBond.pause")
    }

    /// `CapacityBond.minBond()` — the global bond floor, independent of the
    /// capacity curve.
    pub async fn min_bond(&self) -> anyhow::Result<U256> {
        CapacityBond::new(self.addrs.capacity_bond, &self.admin)
            .minBond()
            .call()
            .await
            .context("read minBond")
    }

    /// `CapacityBond.isActive(operator)` — the full active-bonder predicate.
    pub async fn is_active(&self, operator: Address) -> anyhow::Result<bool> {
        CapacityBond::new(self.addrs.capacity_bond, &self.admin)
            .isActive(operator)
            .call()
            .await
            .context("read isActive")
    }

    /// `CapacityBond.getNodeByAddress(operator).active` — raw registered-set
    /// membership, NOT the composite [`Self::is_active`]. An operator can be
    /// registered while `isActive` is false (bond under `minBond`, or a request
    /// in flight), and it is this flag that `deregisterNode` gates on, so a
    /// deregistration journey has to assert against it (#1359).
    pub async fn is_registered(&self, operator: Address) -> anyhow::Result<bool> {
        Ok(CapacityBond::new(self.addrs.capacity_bond, &self.admin)
            .getNodeByAddress(operator)
            .call()
            .await
            .context("read getNodeByAddress")?
            .active)
    }

    /// TOKEN balance of `who` (base units).
    pub async fn token_balance(&self, who: Address) -> anyhow::Result<U256> {
        Erc20::new(self.addrs.token, &self.admin)
            .balanceOf(who)
            .call()
            .await
            .context("read TOKEN balance")
    }
}

/// Build the contracts with `forge build` (a warm no-op once they are built)
/// and return the `contracts/` directory, whose `out/` then holds a current
/// artifact for every contract. Requires `forge` on `PATH`.
pub async fn build_contracts() -> anyhow::Result<PathBuf> {
    let contracts = contracts_dir()?;
    forge_build(&contracts).await?;
    Ok(contracts)
}

/// `crates/<package>/ → ../../contracts`, from the running test's package
/// directory. `CARGO_MANIFEST_DIR` is read at run time, where cargo and nextest
/// set it for every test process, not with `env!`: a compile-time read embeds
/// the checkout path in this library, so a compiler cache rebuilds it, and every
/// test binary that links it, in each new worktree.
fn contracts_dir() -> anyhow::Result<PathBuf> {
    let package_dir = std::env::var_os("CARGO_MANIFEST_DIR")
        .context("CARGO_MANIFEST_DIR is unset: run the test under cargo or nextest")?;
    Path::new(&package_dir)
        .join("../../contracts")
        .canonicalize()
        .context("resolve contracts dir")
}

/// Scale a forge wall-clock budget for the runtime environment: `base` locally,
/// `base * CI_TIMEOUT_MULTIPLIER` under CI. GitHub Actions sets `CI`; we only
/// read the environment here — `set_var` is forbidden repo-wide (and is
/// `unsafe` in edition 2024). See [`scale_for_ci`] for the pure decision.
fn ci_scaled(base: Duration) -> Duration {
    scale_for_ci(base, std::env::var_os("CI").is_some())
}

/// Pure timeout-scaling decision, split out from the environment read so it can
/// be unit-tested without touching global process state.
fn scale_for_ci(base: Duration, is_ci: bool) -> Duration {
    if is_ci {
        base * CI_TIMEOUT_MULTIPLIER
    } else {
        base
    }
}

/// Run a `forge` subprocess under a wall-clock `timeout`, SIGKILLing it on
/// overrun. `Ok(Ok)` = process exited (inspect status); `Ok(Err)` = stalled and
/// killed (retryable); `Err` = could not spawn (deterministic, fail fast).
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
/// completed (a transient broadcast-phase hiccup, retryable) rather than a
/// deterministic revert before the body ran.
fn forge_script_body_completed(stdout: &[u8]) -> bool {
    String::from_utf8_lossy(stdout).contains("Script ran successfully")
}

async fn forge_build(contracts: &Path) -> anyhow::Result<()> {
    for attempt in 1..=BUILD_ATTEMPTS {
        let mut build_cmd = tokio::process::Command::new("forge");
        build_cmd.current_dir(contracts).args(["build"]);
        match forge_output(build_cmd, ci_scaled(FORGE_BUILD_TIMEOUT), "forge build").await? {
            Ok(out) if out.status.success() => return Ok(()),
            // A non-zero exit is a deterministic compile error — retrying wastes
            // attempts, so surface it immediately with diagnostics.
            Ok(out) => anyhow::bail!(
                "forge build failed:\n{}\n{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            ),
            Err(timeout) => {
                if attempt == BUILD_ATTEMPTS {
                    anyhow::bail!(
                        "forge build failed after {BUILD_ATTEMPTS} attempts; \
                         final attempt timed out after {timeout:?}"
                    );
                }
                tracing::warn!(
                    "forge build attempt {attempt}/{BUILD_ATTEMPTS} timed out after {timeout:?}, retrying"
                );
            }
        }
    }
    anyhow::bail!("BUILD_ATTEMPTS must be >= 1 (was {BUILD_ATTEMPTS})")
}

/// Read the `bytecode.object` creation code from a compiled forge artifact.
fn artifact_bytecode(artifact: &Path) -> anyhow::Result<String> {
    let bytes = std::fs::read(artifact)
        .with_context(|| format!("read forge artifact at {}", artifact.display()))?;
    let json: serde_json::Value = serde_json::from_slice(&bytes)?;
    json.get("bytecode")
        .and_then(|b| b.get("object"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .with_context(|| format!("artifact {} missing bytecode.object", artifact.display()))
}

/// Deploy the mintable mock USDC from its compiled artifact bytecode.
async fn deploy_mock_usdc<P: Provider>(provider: &P, contracts: &Path) -> anyhow::Result<Address> {
    deploy_artifact(provider, contracts, "MintableUSDC", &[]).await
}

/// Deploy contract `name` from its compiled artifact (`out/{name}.sol/{name}.json`)
/// with `ctor_args` — the ABI-encoded constructor parameters — appended to its
/// creation code, and return the new contract's address.
async fn deploy_artifact<P: Provider>(
    provider: &P,
    contracts: &Path,
    name: &str,
    ctor_args: &[u8],
) -> anyhow::Result<Address> {
    let code_hex = artifact_bytecode(&contracts.join(format!("out/{name}.sol/{name}.json")))?;
    let code: Bytes = code_hex
        .parse()
        .with_context(|| format!("parse {name} bytecode"))?;
    let receipt = provider
        .send_transaction(
            TransactionRequest::default().with_deploy_code([code.as_ref(), ctor_args].concat()),
        )
        .await
        .with_context(|| format!("{name} deploy send"))?
        .get_receipt()
        .await
        .with_context(|| format!("{name} deploy receipt"))?;
    crate::ensure_mined(&receipt, &format!("{name} deploy"))?;
    receipt
        .contract_address
        .ok_or_else(|| anyhow::anyhow!("{name} deploy produced no contract address"))
}

/// A one-time protocol deployment shared by every journey in a test run: the
/// path to the anvil state snapshot to replay, plus the addresses read back from
/// the deploy manifest. Produced by [`ensure_shared_deployment`].
struct SharedDeployment {
    state_path: PathBuf,
    manifest: Manifest,
}

/// The addresses a deploy manifest records.
struct Manifest {
    /// The protocol contracts the fixture has typed handles for.
    addrs: ContractAddrs,
    /// The settlement USDC (`externalDeps.usdc`).
    usdc: Address,
    /// Every `contracts` entry as `(name, address)`, sorted by name.
    contracts: Vec<(String, Address)>,
}

/// Deploy the protocol once per test run and return handles to the snapshot the
/// per-journey fixtures replay.
///
/// nextest runs each journey in its own process, so this coordinates *across
/// processes*: an advisory file lock elects one deployer while its siblings
/// block, and the cache files it commits are how the result crosses the process
/// boundary. A warm cache (this run or a prior one with identical artifacts)
/// short-circuits before the lock.
async fn ensure_shared_deployment(contracts: &Path) -> anyhow::Result<SharedDeployment> {
    // Every process builds contracts: the cache key is computed from the
    // compiled artifacts, and a cache miss needs them to deploy. Warm builds are
    // a sub-second no-op (CI hoists a cold build into a one-time job step).
    forge_build(contracts).await?;

    let key = artifact_cache_key(contracts)?;
    let dir = cache_dir(contracts);
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("create e2e chain cache dir {}", dir.display()))?;
    let paths = CachePaths {
        state: dir.join(format!("state-{key}.json")),
        payment: dir.join(format!("payment-{key}.json")),
        manifest: dir.join(format!("manifest-{key}.json")),
    };

    if let Some(shared) = load_cached_deployment(&paths)? {
        return Ok(shared);
    }

    // Elect a single deployer. `File::lock` is a blocking flock/LockFileEx held
    // for the whole deploy and released when the file drops (including on a crash
    // or SIGKILL), so a dead winner never wedges the siblings. Take it off the
    // async runtime.
    let lock_path = dir.join(format!("bootstrap-{key}.lock"));
    let lock = tokio::task::spawn_blocking(move || -> anyhow::Result<File> {
        let file = File::create(&lock_path)
            .with_context(|| format!("create bootstrap lock {}", lock_path.display()))?;
        file.lock().context("acquire bootstrap deploy lock")?;
        Ok(file)
    })
    .await
    .context("join bootstrap lock task")??;

    // Re-check under the lock: a sibling that held it before us has already
    // populated the cache, so we load rather than redeploy.
    let shared = match load_cached_deployment(&paths)? {
        Some(shared) => shared,
        None => deploy_and_snapshot(contracts, &paths).await?,
    };
    drop(lock);
    Ok(shared)
}

/// Where the shared deployment's cache files live: the anvil state snapshot,
/// the payment-chain manifest, and the governance manifest, written in that
/// order so the governance manifest marks a complete commit.
struct CachePaths {
    state: PathBuf,
    payment: PathBuf,
    manifest: PathBuf,
}

/// Run the one-time deploy in a throwaway anvil and commit its state snapshot +
/// manifests to the cache. The caller holds the bootstrap lock for the
/// duration.
///
/// The fixture chain is both the governance chain and the payment chain, as
/// `contracts/dev-deploy.sh` sets it up: one `LoopbackCctp` plays CCTP V2 on
/// both sides (domain 0), `DeployProtocol` deploys the governance suite, and
/// `DeployPaymentPool` deploys the `PaymentPool` that sweeps to the suite's
/// `CctpSweepReceiver`.
async fn deploy_and_snapshot(
    contracts: &Path,
    paths: &CachePaths,
) -> anyhow::Result<SharedDeployment> {
    let forge_manifest = contracts.join(format!("deployments/{E2E_CHAIN_ID}.json"));
    let forge_payment = contracts.join(format!("deployments/{E2E_CHAIN_ID}-payment.json"));
    // The guard removes the forge manifests on drop; the cache keeps its own
    // copies.
    let anvil = spawn_anvil(vec![forge_manifest.clone(), forge_payment.clone()]).await?;

    // Mock USDC and the loopback CCTP first, then the protocol with the
    // initial TOKEN supply held by the admin EOA so it can distribute bond
    // stake to N operators.
    let usdc = deploy_mock_usdc(&anvil.admin, contracts).await?;
    let cctp = deploy_artifact(
        &anvil.admin,
        contracts,
        "LoopbackCctp",
        &(0u32,).abi_encode_params(),
    )
    .await?;
    let token_holder = admin_address()?;
    let deps = DeployDeps {
        usdc,
        cctp,
        initial_token_holder: token_holder,
    };
    run_deploy_script(&anvil.admin, contracts, &anvil.rpc_url, &deps).await?;
    let receiver = manifest_contract(&forge_manifest, "CctpSweepReceiver")?;
    run_payment_pool_script(contracts, &anvil.rpc_url, &deps, receiver).await?;

    let manifest = read_manifest(&forge_manifest, &forge_payment)?;
    anyhow::ensure!(
        manifest.usdc == usdc,
        "manifest externalDeps.usdc {} disagrees with deployed mock USDC {usdc}",
        manifest.usdc
    );

    // Snapshot the fully-deployed chain. `anvil_dumpState` returns a gzip-hex
    // blob that `anvil_loadState` replays verbatim (the CLI `--dump-state` /
    // `--load-state` files use a different, incompatible encoding).
    let state_hex: String = anvil
        .admin
        .raw_request("anvil_dumpState".into(), ())
        .await
        .context("anvil_dumpState")?;

    // Order matters: the state snapshot is written first and the manifest last,
    // so any reader that observes the manifest is guaranteed a complete snapshot
    // beside it. Each write is an atomic rename, so neither file is ever seen
    // half-written.
    write_atomic(&paths.state, state_hex.as_bytes())?;
    let payment_bytes = std::fs::read(&forge_payment)
        .with_context(|| format!("re-read forge manifest {}", forge_payment.display()))?;
    write_atomic(&paths.payment, &payment_bytes)?;
    let manifest_bytes = std::fs::read(&forge_manifest)
        .with_context(|| format!("re-read forge manifest {}", forge_manifest.display()))?;
    write_atomic(&paths.manifest, &manifest_bytes)?;

    Ok(SharedDeployment {
        state_path: paths.state.clone(),
        manifest,
    })
}

/// Load a previously committed deployment, or `None` if the cache is absent or
/// incomplete (the governance manifest is the commit marker, but require every
/// file so a torn write is never mistaken for a hit).
fn load_cached_deployment(paths: &CachePaths) -> anyhow::Result<Option<SharedDeployment>> {
    if !nonempty_file(&paths.state)
        || !nonempty_file(&paths.payment)
        || !nonempty_file(&paths.manifest)
    {
        return Ok(None);
    }
    Ok(Some(SharedDeployment {
        state_path: paths.state.clone(),
        manifest: read_manifest(&paths.manifest, &paths.payment)?,
    }))
}

/// Replay a dumped chain state into a fresh anvil via `anvil_loadState`.
async fn load_state_snapshot(provider: &DynProvider, state_path: &Path) -> anyhow::Result<()> {
    let state_hex = std::fs::read_to_string(state_path)
        .with_context(|| format!("read chain state snapshot {}", state_path.display()))?;
    let loaded: bool = provider
        .raw_request("anvil_loadState".into(), (state_hex.trim(),))
        .await
        .context("anvil_loadState")?;
    anyhow::ensure!(loaded, "anvil_loadState returned false");
    Ok(())
}

/// `true` when `path` exists and is non-empty. An atomic-rename commit means a
/// present-and-non-empty file is also a complete one.
fn nonempty_file(path: &Path) -> bool {
    std::fs::metadata(path).is_ok_and(|m| m.len() > 0)
}

/// Write `bytes` to `path` atomically: write a sibling temp file, then rename
/// over `path`. A reader therefore sees either the old file or the whole new
/// one, never a partial write.
fn write_atomic(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let tmp = path.with_extension(format!("tmp.{}", std::process::id()));
    std::fs::write(&tmp, bytes).with_context(|| format!("write temp {}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

/// The per-checkout cache directory for shared chain state, under the system
/// temp dir. Keyed by a hash of the contracts path so sibling worktrees (and
/// unrelated checkouts) never share a cache.
fn cache_dir(contracts: &Path) -> PathBuf {
    let path_key = short_hash(contracts.to_string_lossy().as_bytes());
    std::env::temp_dir()
        .join("decdn-e2e-chaincache")
        .join(path_key)
}

/// A cache key that changes whenever the deployed state would: the compiled
/// creation code of the mock USDC and the deploy script (which embeds every
/// contract it `new`s), plus the deploy inputs the script reads from the
/// environment. `SCHEMA` is bumped by hand when the snapshot encoding or this
/// fixture's deploy wiring changes in a way the bytecode alone does not capture.
fn artifact_cache_key(contracts: &Path) -> anyhow::Result<String> {
    const SCHEMA: &str = "v3";
    let usdc_bc = artifact_bytecode(&contracts.join("out/MintableUSDC.sol/MintableUSDC.json"))?;
    let cctp_bc = artifact_bytecode(&contracts.join("out/LoopbackCctp.sol/LoopbackCctp.json"))?;
    let deploy_bc =
        artifact_bytecode(&contracts.join("out/DeployProtocol.s.sol/DeployProtocol.json"))?;
    let pool_bc =
        artifact_bytecode(&contracts.join("out/DeployPaymentPool.s.sol/DeployPaymentPool.json"))?;
    let mut buf = Vec::new();
    buf.extend_from_slice(SCHEMA.as_bytes());
    buf.extend_from_slice(&E2E_CHAIN_ID.to_le_bytes());
    buf.extend_from_slice(usdc_bc.as_bytes());
    buf.extend_from_slice(cctp_bc.as_bytes());
    buf.extend_from_slice(deploy_bc.as_bytes());
    buf.extend_from_slice(pool_bc.as_bytes());
    buf.extend_from_slice(terms_hash().as_slice());
    buf.extend_from_slice(admin_address()?.as_slice());
    buf.extend_from_slice(DEPLOYER_ADDR.as_bytes());
    Ok(short_hash(&buf))
}

/// First 8 bytes of `keccak256(bytes)` as lowercase hex — a short, collision-
/// resistant-enough filename component for cache keying.
fn short_hash(bytes: &[u8]) -> String {
    let digest = keccak256(bytes);
    alloy::hex::encode(digest.get(..8).unwrap_or(digest.as_slice()))
}

/// Run `forge script DeployProtocol.s.sol` against the anvil RPC, retrying the
/// two transient failure classes (stall #785, broadcast-phase non-zero exit
/// #883) and failing fast on a deterministic revert.
///
/// Between retries the broadcast lane is reset: anvil's pool is drained and the
/// chain reverted to a snapshot taken *before* the first attempt. This is
/// load-bearing: a stalled `--broadcast` run is SIGKILLed mid-flight, having
/// already advanced deployer dev-#0's nonce and mined a partial deploy, so a
/// naive re-run re-broadcasts the same nonces and dies with `-32003 replacement
/// transaction underpriced`. Mock USDC is deployed *before* the snapshot, so it
/// survives the revert, and `admin` (anvil dev #1) sends no tx between the
/// snapshot and the loop.
async fn run_deploy_script(
    admin: &DynProvider,
    contracts: &Path,
    rpc_url: &str,
    deps: &DeployDeps,
) -> anyhow::Result<()> {
    let DeployDeps {
        usdc,
        cctp,
        initial_token_holder,
    } = *deps;
    let mut snapshot = evm_snapshot(admin).await?;
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
            .env("CCTP_MESSAGE_TRANSMITTER", cctp.to_string())
            .env("CCTP_TOKEN_MESSENGER", cctp.to_string())
            .env("INITIAL_TOKEN_HOLDER", initial_token_holder.to_string())
            .env("EMERGENCY_MULTISIG", DEPLOYER_ADDR)
            // DeployProtocol.s.sol requires a genesis VETTER_ROLE holder (a deploy
            // with none can vet no publisher and is a governance deadlock). The
            // deployer plays it here; `vet_publisher` grants the role to the
            // Timelock and vets through it regardless.
            .env("INITIAL_VETTER", DEPLOYER_ADDR)
            // ADR 019 § Terms Acceptance — DeployProtocol.s.sol requires a
            // non-zero genesis terms hash (CapacityBond rejects the zero
            // sentinel); registration reads it back from the contract.
            //
            // Deployed as the REAL terms hash, not a sentinel: any journey that
            // registers through the `decdn` CLI (rather than through
            // `onboard_operator`, which signs whatever the contract holds) goes
            // through `terms::ensure_accepted`, which refuses to sign when the
            // binary's embedded terms do not hash to the network's value. A
            // sentinel would make every such journey unrunnable.
            .env("CURRENT_TERMS_HASH", terms_hash().to_string())
            .env("FORCE_OVERWRITE_MANIFEST", "true");
        match forge_output(
            cmd,
            ci_scaled(DEPLOY_TIMEOUT),
            "forge script DeployProtocol",
        )
        .await?
        {
            Ok(out) if out.status.success() => return Ok(()),
            Ok(out) if forge_script_body_completed(&out.stdout) => {
                if attempt == DEPLOY_ATTEMPTS {
                    anyhow::bail!(
                        "forge script DeployProtocol failed after {DEPLOY_ATTEMPTS} attempts; \
                         final attempt completed the body but exited non-zero during broadcast:\n{}\n{}",
                        String::from_utf8_lossy(&out.stdout),
                        String::from_utf8_lossy(&out.stderr)
                    );
                }
                tracing::warn!(
                    "forge script DeployProtocol attempt {attempt}/{DEPLOY_ATTEMPTS} broadcast hiccup, retrying"
                );
            }
            Ok(out) => anyhow::bail!(
                "forge script DeployProtocol exited non-zero before the body completed (revert/bug):\n{}\n{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            ),
            Err(timeout) => {
                if attempt == DEPLOY_ATTEMPTS {
                    anyhow::bail!(
                        "forge script DeployProtocol failed after {DEPLOY_ATTEMPTS} attempts; \
                         final attempt stalled (timed out after {timeout:?})"
                    );
                }
                tracing::warn!(
                    "forge script DeployProtocol attempt {attempt}/{DEPLOY_ATTEMPTS} stalled, retrying"
                );
            }
        }
        // Reached only on a retryable, non-final attempt (the branches above
        // either returned, bailed, or warned). Roll the killed attempt's partial
        // deploy back to a clean deployer nonce, then let a transient contention
        // spike on the shared runner clear before spawning the next forge.
        snapshot = reset_broadcast_lane(admin, snapshot).await?;
        tokio::time::sleep(DEPLOY_RETRY_BACKOFF * u32::try_from(attempt).unwrap_or(u32::MAX)).await;
    }
    anyhow::bail!("DEPLOY_ATTEMPTS must be >= 1 (was {DEPLOY_ATTEMPTS})")
}

/// What the deploy scripts take from the fixture: the mock USDC, the
/// `LoopbackCctp` that plays both CCTP V2 sides, and the initial TOKEN holder.
#[derive(Debug, Clone, Copy)]
struct DeployDeps {
    usdc: Address,
    cctp: Address,
    initial_token_holder: Address,
}

/// Run `forge script DeployPaymentPool.s.sol` against the anvil RPC: one
/// `PaymentPool` on the fixture chain, sweeping through the loopback CCTP to
/// `receiver`, the governance suite's `CctpSweepReceiver`. It writes
/// `deployments/<chainId>-payment.json`.
async fn run_payment_pool_script(
    contracts: &Path,
    rpc_url: &str,
    deps: &DeployDeps,
    receiver: Address,
) -> anyhow::Result<()> {
    let mut cmd = tokio::process::Command::new("forge");
    cmd.current_dir(contracts)
        .args([
            "script",
            "script/DeployPaymentPool.s.sol:DeployPaymentPool",
            "--rpc-url",
            rpc_url,
            "--broadcast",
            "--private-key",
            DEPLOYER_KEY,
            "--sender",
            DEPLOYER_ADDR,
        ])
        .env("USDC_ADDRESS", deps.usdc.to_string())
        .env("CCTP_TOKEN_MESSENGER", deps.cctp.to_string())
        .env("SWEEP_RECIPIENT", receiver.to_string())
        .env("PAUSER", DEPLOYER_ADDR)
        // A journey's operators earn a few µUSDC, so any pending share sweeps
        // ([`ChainFixture::sweep`]).
        .env("MIN_SWEEP", "1")
        .env("FORCE_OVERWRITE_MANIFEST", "true");
    match forge_output(
        cmd,
        ci_scaled(DEPLOY_TIMEOUT),
        "forge script DeployPaymentPool",
    )
    .await?
    {
        Ok(out) if out.status.success() => Ok(()),
        Ok(out) => anyhow::bail!(
            "forge script DeployPaymentPool failed:\n{}\n{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        ),
        Err(timeout) => anyhow::bail!("forge script DeployPaymentPool timed out after {timeout:?}"),
    }
}

/// Give the next deploy attempt a genuinely clean broadcast lane: drain anvil's
/// transaction pool, then revert to `snapshot` and re-snapshot.
///
/// The drain has to come first, and has to be *observed* rather than assumed.
/// `evm_revert` rewinds chain state but leaves the pool untouched, so a
/// transaction the SIGKILLed forge had already put on the wire is still pending
/// afterwards: it reads as a clean nonce immediately after the revert, then
/// mines into the reverted chain and re-advances the deployer nonce — after the
/// fresh snapshot was taken. Every later attempt then broadcasts against a nonce
/// anvil has already passed and dies fast with `-32003 nonce too low` /
/// `transaction already imported`, burning the whole retry budget (#785). Draining
/// until the pool stays empty, and only then reverting, wipes both the pending
/// stragglers and any that mined while we waited.
///
/// A pool that will not drain within the budget warns rather than bails. It is
/// tempting to fail fast — a straggler that outlives the drain can still poison
/// the snapshot — but that trades a *maybe* for a certain failure. The condition
/// means extreme runner contention, which is transient and exactly what the
/// caller's escalating backoff exists to ride out; bailing here would forfeit
/// the remaining attempts instead. It is also not proof of poisoning: a
/// straggler that mines *before* the revert is rolled back by it, which is the
/// common case. So make it loud enough to explain a later `nonce too low`, and
/// let the retry budget do its job.
async fn reset_broadcast_lane(provider: &DynProvider, snapshot: String) -> anyhow::Result<String> {
    let mut drained = false;
    for _ in 0..POOL_DRAIN_POLLS {
        drop_all_transactions(provider).await?;
        // SIGKILL stops forge writing more, but bytes already in the socket are
        // still being parsed; let them land so the next drop catches them.
        tokio::time::sleep(POOL_DRAIN_SETTLE).await;
        if pool_is_empty(provider).await? {
            drained = true;
            break;
        }
    }
    if !drained {
        // Deliberately not fatal — see this function's doc comment.
        tracing::warn!(
            "anvil's pool still had transactions after {POOL_DRAIN_POLLS} drops; if one of \
             them mines after the snapshot below, the next attempt will fail fast with \
             `nonce too low`"
        );
    }
    evm_revert_and_snapshot(provider, snapshot).await
}

/// Drop every transaction currently in anvil's pool (pending and queued).
async fn drop_all_transactions(provider: &DynProvider) -> anyhow::Result<()> {
    provider
        .raw_request::<_, ()>("anvil_dropAllTransactions".into(), ())
        .await
        .context("anvil_dropAllTransactions")
}

/// `true` when anvil's pool holds no pending or queued transactions.
async fn pool_is_empty(provider: &DynProvider) -> anyhow::Result<bool> {
    let status: serde_json::Value = provider
        .raw_request("txpool_status".into(), ())
        .await
        .context("txpool_status")?;
    Ok(hex_quantity(&status, "pending")? == 0 && hex_quantity(&status, "queued")? == 0)
}

/// Read `field` from a `txpool_status` response as a hex quantity (`"0x1"`).
fn hex_quantity(status: &serde_json::Value, field: &str) -> anyhow::Result<u64> {
    let raw = status
        .get(field)
        .and_then(serde_json::Value::as_str)
        .with_context(|| format!("txpool_status missing string field `{field}`"))?;
    let digits = raw.strip_prefix("0x").unwrap_or(raw);
    u64::from_str_radix(digits, 16)
        .with_context(|| format!("txpool_status `{field}` is not a hex quantity: {raw}"))
}

/// Take an `evm_snapshot`, returning the anvil snapshot id (a hex quantity).
async fn evm_snapshot(provider: &DynProvider) -> anyhow::Result<String> {
    provider
        .raw_request("evm_snapshot".into(), ())
        .await
        .context("evm_snapshot")
}

/// Revert anvil to `snapshot` (dropping everything mined since it was taken) and
/// return a fresh snapshot id, since `evm_revert` invalidates the id it consumes.
async fn evm_revert_and_snapshot(
    provider: &DynProvider,
    snapshot: String,
) -> anyhow::Result<String> {
    let reverted: bool = provider
        .raw_request("evm_revert".into(), (snapshot,))
        .await
        .context("evm_revert")?;
    anyhow::ensure!(reverted, "evm_revert returned false (unknown snapshot id)");
    evm_snapshot(provider).await
}

/// The canonical operator terms, embedded from `crates/cli/TERMS.md` — the same
/// file `decdn_cli::commands::terms::TERMS_TEXT` embeds.
///
/// Reaching into another package's directory is legitimate here only because
/// `decdn-e2e` is `publish = false`, which the packaging check exempts; a
/// publishable crate doing this would ship a `.crate` missing the file.
///
/// Duplicated from the CLI rather than shared: nothing may depend on
/// `decdn-cli` (it is a binary sink), and moving the terms into `decdn-common`
/// is a wider refactor than a fixture warrants. The duplication is of the
/// *recipe*, and it is self-policing — a change to how the CLI derives its hash
/// makes every CLI registration journey fail immediately, with the mismatch
/// named in the error.
const TERMS_TEXT: &str = include_str!("../../cli/TERMS.md");

/// `keccak256` of the embedded terms — the genesis `currentTermsHash` the
/// fixture chain deploys with, and the value a CLI registration must match.
fn terms_hash() -> B256 {
    alloy::primitives::keccak256(TERMS_TEXT.as_bytes())
}

/// The admin EOA address, derived from [`ADMIN_KEY`] so it is always the address
/// of the wallet the fixture's provider signs with (see [`spawn_anvil`]).
fn admin_address() -> anyhow::Result<Address> {
    let signer: PrivateKeySigner = ADMIN_KEY.parse().context("parse admin key")?;
    Ok(signer.address())
}

/// Pack a region string (ISO 3166-1 alpha-2, or any ≤32-byte label) into the
/// left-aligned, zero-padded `bytes32` key `ContentBlacklist` and
/// `RegionScopeLib` use — matching Solidity's `bytes32("literal")`.
#[must_use]
pub fn region_key(region: &str) -> B256 {
    B256::right_padding_from(region.as_bytes())
}

/// `keccak256("EMERGENCY_MULTISIG_ROLE")` — `registerRegionalBody` verifies its
/// comparison-side address holds this.
fn emergency_multisig_role() -> B256 {
    keccak256(b"EMERGENCY_MULTISIG_ROLE")
}

/// The `contracts.<name>` address the manifest at `path` records.
fn manifest_contract(path: &Path, name: &str) -> anyhow::Result<Address> {
    let json: serde_json::Value = serde_json::from_slice(&std::fs::read(path)?)?;
    json.get("contracts")
        .and_then(|contracts| contracts.get(name))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("manifest {} missing contracts.{name}", path.display()))?
        .parse()
        .with_context(|| format!("parse manifest address contracts.{name}"))
}

/// Read the protocol contract addresses and the settlement USDC from the
/// governance deploy manifest at `path`, and the `PaymentPool` from the
/// payment-chain manifest at `payment`. The mock USDC is recorded under
/// `externalDeps.usdc` — the same address the fixture deployed and fed to the
/// script as `USDC_ADDRESS`. The `contracts` list holds both manifests'
/// entries.
fn read_manifest(path: &Path, payment: &Path) -> anyhow::Result<Manifest> {
    let json: serde_json::Value = serde_json::from_slice(&std::fs::read(path)?)?;
    let contracts = json
        .get("contracts")
        .ok_or_else(|| anyhow::anyhow!("manifest missing `contracts` map"))?;
    let get = |k: &str| -> anyhow::Result<Address> {
        contracts
            .get(k)
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("manifest missing contracts.{k}"))?
            .parse()
            .with_context(|| format!("parse manifest address contracts.{k}"))
    };
    let payment_pool = manifest_contract(payment, "PaymentPool")?;
    let addrs = ContractAddrs {
        capacity_bond: get("CapacityBond")?,
        payment_pool,
        fee_router: get("FeeRouter")?,
        token: get("Token")?,
        governor: get("DecdnGovernor")?,
        timelock: get("TimelockController")?,
        publisher_registry: get("PublisherRegistry")?,
        origin_assignment: get("OriginAssignment")?,
        manual_vetting_policy: get("ManualVettingPolicy")?,
        content_blacklist: get("ContentBlacklist")?,
        cctp_sweep_receiver: get("CctpSweepReceiver")?,
    };
    let usdc: Address = json
        .get("externalDeps")
        .and_then(|d| d.get("usdc"))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("manifest missing externalDeps.usdc"))?
        .parse()
        .context("parse manifest externalDeps.usdc")?;
    let mut all = contracts
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("manifest `contracts` is not a map"))?
        .keys()
        .map(|name| Ok((name.clone(), get(name)?)))
        .collect::<anyhow::Result<Vec<_>>>()?;
    all.push(("PaymentPool".to_owned(), payment_pool));
    all.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
    Ok(Manifest {
        addrs,
        usdc,
        contracts: all,
    })
}

#[cfg(test)]
mod tests;
