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
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::Duration;

use alloy::network::{EthereumWallet, TransactionBuilder};
use alloy::primitives::{Address, B256, Bytes, U256, keccak256};
use alloy::providers::{DynProvider, Provider, ProviderBuilder};
use alloy::rpc::types::TransactionRequest;
use alloy::signers::SignerSync;
use alloy::signers::local::PrivateKeySigner;
use alloy::sol_types::{SolEvent, SolValue};
use anyhow::Context;
use decdn_incentive::probe_sig::ProbeSlashData;
use decdn_incentive::stream_sig::StreamSlashData;
use decdn_incentive::{
    bind_node_id_domain, binding_signing_hash, node_register, register_node_signing_hash,
    slash_judge_domain,
};

use crate::bindings::{
    AccessControl, CapacityBond, ContentBlacklist, ContentBlacklistOrigin, DecdnGovernor, Erc20,
    ManualVettingPolicy, OriginAssignment, PublisherRegistry, SlashAppeal, SlashJudge,
    TimelockController,
};

/// The single chain id every anvil-e2e chain runs on: the one-time protocol
/// deploy and every per-journey fixture that loads its state snapshot.
///
/// A fixed value is what makes the state snapshot reusable. EIP-712 domain
/// separators bind to `block.chainid`, so the fixture that loads the snapshot
/// must run the same chain id the snapshot was deployed under, or every signed
/// voucher / node-id bind / slash attestation the journeys build would verify
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
// for a slow shared runner, but the base is the pre-#1384 value again: one
// uncontended deploy is comfortably fast.
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
/// Re-pick the ephemeral port and re-spawn anvil this many times when it dies at
/// startup (the `free_port` TOCTOU: another process claimed the port first).
const ANVIL_ATTEMPTS: usize = 3;

/// The `SlashJudge._verifyPair` offense a signed probe/stream pair can prove
/// (#1042). Mirrors the paired entry of `ISlashJudge.OffenseType`; `Blacklist`
/// is excluded because it takes a single response, not a pair, and has its own
/// entry point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Offense {
    /// `stream.ok && stream.ratePerMb > probe.ratePerMb` within the 30s window —
    /// quoted low to win selection, then delivered at a higher rate.
    RateManipulation,
}

impl Offense {
    /// The generated, contract-canonical `OffenseType`. Going through the `sol!`
    /// enum rather than a hand-written ordinal keeps this pinned to the one
    /// declaration a Solidity reorder would have to touch anyway
    /// (`decdn_incentive::slash_judge`) — a stale literal here would instead
    /// corrupt `evidenceHash` and surface as a misleading `NoCommitment()`.
    const fn offense_type(self) -> SlashJudge::OffenseType {
        match self {
            Self::RateManipulation => SlashJudge::OffenseType::RateManipulation,
        }
    }

    /// Contract-canonical `OffenseType` discriminant, folded into `evidenceHash`.
    const fn discriminant(self) -> u8 {
        self.offense_type() as u8
    }

    /// The reveal entry point, for error context.
    const fn entry_point(self) -> &'static str {
        match self {
            Self::RateManipulation => "submitRateChallenge",
        }
    }
}

/// The two signed wire messages a `_verifyPair` challenge is built from, exactly
/// as the daemon emitted them — `slash_sig` included and never re-signed. Borrowed
/// so a negative case can perturb one field of a clone and resubmit (#1042).
///
/// Deliberately **unvalidated**: no same-hash, same-signer, or window check. The
/// judge is the only arbiter, and the negative journeys submit deliberately
/// incoherent pairs (a 41s-apart pair, a probe re-signed by an impostor), so a
/// validating constructor would make half the coverage unwritable.
#[derive(Debug, Clone, Copy)]
pub struct EvidencePair<'a> {
    /// The signed `cdn/probe/v1` response.
    pub probe: &'a decdn_protocol::ProbeResponse,
    /// The signed `cdn/client/v1` open-stage response.
    pub stream: &'a decdn_protocol::client::StreamResponse,
}

/// Deployed protocol contract addresses, read from the forge-script manifest.
#[derive(Debug, Clone, Copy)]
pub struct ContractAddrs {
    pub capacity_bond: Address,
    pub payment_pool: Address,
    pub fee_router: Address,
    pub token: Address,
    pub slash_judge: Address,
    pub slash_appeal: Address,
    pub governor: Address,
    /// `TimelockController` — holds `GOVERNANCE_ROLE` + `DEFAULT_ADMIN_ROLE` on
    /// the governed contracts after the `DeployProtocol` handoff. The blacklist
    /// journeys drive governance actions by impersonating it (anvil), since the
    /// deployer keeps no privileged roles; the G-NODE-05 grant executes through
    /// it via a real Governor proposal.
    pub timelock: Address,
    pub publisher_registry: Address,
    pub origin_assignment: Address,
    /// `ManualVettingPolicy` — the genesis vetting policy `OriginAssignment`
    /// reads. The fixture vets publishers here (see [`ChainFixture::vet_publisher`]).
    pub manual_vetting_policy: Address,
    pub content_blacklist: Address,
}

/// Kills the spawned `anvil` on drop — a panicking assertion never leaks the
/// process. `manifest` is `Some` only for the throwaway anvil the one-time
/// deploy runs in: dropping it removes the (gitignored) `deployments/*.json` the
/// forge script wrote. Per-journey anvils load a state snapshot instead of
/// deploying, so they write no manifest and carry `None`.
#[derive(Debug)]
struct AnvilGuard {
    child: Child,
    manifest: Option<PathBuf>,
}

impl Drop for AnvilGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(manifest) = &self.manifest {
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

/// Bring up an `anvil` on a fresh ephemeral port at [`E2E_CHAIN_ID`] and wait
/// for its RPC, retrying the `free_port` TOCTOU (another process claims the port
/// in the gap before anvil binds, so anvil exits at startup — re-pick and retry
/// rather than fail the fixture).
///
/// `manifest` is the `deployments/*.json` path whose cleanup the guard owns —
/// `Some` for the throwaway anvil the one-time deploy writes a manifest into,
/// `None` for a per-journey anvil that loads a snapshot and writes none.
async fn spawn_anvil(manifest: Option<PathBuf>) -> anyhow::Result<AnvilProcess> {
    let admin_signer: PrivateKeySigner = ADMIN_KEY.parse().context("parse admin key")?;
    let mut attempt = 0;
    loop {
        attempt += 1;
        let port = crate::free_port()?;
        let rpc_url = format!("http://127.0.0.1:{port}");
        let child = Command::new("anvil")
            .args([
                "--port",
                &port.to_string(),
                "--chain-id",
                &E2E_CHAIN_ID.to_string(),
                "--silent",
            ])
            .spawn()
            .context("spawn anvil (is foundry installed?)")?;
        let mut guard = AnvilGuard {
            child,
            manifest: manifest.clone(),
        };

        let url: reqwest::Url = rpc_url.parse().context("parse anvil rpc url")?;
        let admin: DynProvider = ProviderBuilder::new()
            .with_simple_nonce_management()
            .wallet(EthereumWallet::from(admin_signer.clone()))
            .connect_http(url.clone())
            .erased();

        // Wait for the RPC to accept requests. A premature anvil exit is almost
        // always the port clash above (retryable); a genuine timeout is a hard
        // failure.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        let up = loop {
            if admin.get_chain_id().await.is_ok() {
                break true;
            }
            if let Ok(Some(status)) = guard.child.try_wait() {
                tracing::warn!(
                    "anvil exited before its RPC came up (status {status}) on attempt \
                     {attempt}/{ANVIL_ATTEMPTS}; retrying on a fresh port"
                );
                break false;
            }
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!("anvil RPC never came up within 20s");
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        };
        if up {
            return Ok(AnvilProcess {
                guard,
                rpc_url,
                url,
                admin,
            });
        }
        // `guard` drops here: kills the dead child before the next attempt.
        if attempt >= ANVIL_ATTEMPTS {
            anyhow::bail!(
                "anvil never became ready after {ANVIL_ATTEMPTS} attempts (exited before its RPC came up each time)"
            );
        }
    }
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

        let anvil = spawn_anvil(None).await?;
        load_state_snapshot(&anvil.admin, &shared.state_path).await?;

        let admin_addr = admin_address()?;
        Ok(Self {
            _anvil: anvil.guard,
            url: anvil.url,
            chain_id: E2E_CHAIN_ID,
            addrs: shared.addrs,
            usdc: shared.usdc,
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

    /// Mock USDC (mintable) the settlement token points at.
    #[must_use]
    pub const fn usdc(&self) -> Address {
        self.usdc
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

    /// Build a wallet-filled provider for `signer` (simple nonce management
    /// mirrors production — a reverted gas-estimate must not leak a cached
    /// nonce and gap the lane).
    #[must_use]
    pub fn provider_for(&self, signer: &PrivateKeySigner) -> DynProvider {
        ProviderBuilder::new()
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
        let domain = bind_node_id_domain(self.chain_id, self.addrs.capacity_bond);
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
    /// upstream `PaymentChannel` and pay for a node-to-node cache-miss pull.
    ///
    /// [`Self::onboard_operator`] funds only the operator's *seller* role (a
    /// TOKEN capacity bond); it never mints the USDC a buyer channel escrows, so
    /// a node whose miss must pay an upstream peer would fail at channel-open.
    /// This mints `usdc` settlement units to the operator address (the daemon
    /// opens buyer channels under the same eth key it bonds and settles with).
    ///
    /// It deliberately does **not** set the `PaymentChannel` allowance: the
    /// daemon does that itself, once, at buyer bootstrap (`blockchain.buyer_max_approve`
    /// defaults to `true`, so `BuyerChannelService::bootstrap` sends the approve
    /// from the node's own wallet before pull-through is provisioned). Approving
    /// here from the same operator key would race the daemon's approve on that
    /// account's nonce, so minting is the fixture's whole job — the approval is
    /// the node's. `usdc` need only cover the buyer deposit
    /// (`blockchain.buyer_working_deposit_micro_usdc`, default 10 USDC) plus any
    /// reactive top-up back toward it; mint generously so a multi-interval pull
    /// never starves the channel.
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

        let domain = bind_node_id_domain(self.chain_id, self.addrs.capacity_bond);
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
    /// Reads `addressToNodeId` rather than `nodeIdOf`, deliberately: this is the
    /// exact mapping `SlashJudge._checkRegistered` resolves through, so an
    /// assertion on it is an assertion about slashability. `nodeIdOf` returns
    /// the same id but pairs it with a liveness flag that the slashing path
    /// ignores.
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
    ///
    /// Note this is **not** the read the slashing path performs.
    /// `SlashJudge._checkRegistered(challengedNode, nodeId)` resolves the
    /// challenged *address* through `nodeIdOf` and compares — it never does a
    /// nodeId → address lookup. So a challenge citing a retired id against a
    /// still-bound operator reverts `NodeIdMismatch`; `NodeNotRegistered` fires
    /// only when the challenged address has no binding at all.
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

    /// Unseat one operator from `namespace`'s active origin set as the namespace
    /// owner. Takes effect immediately and emits `OriginRemoved` — the event
    /// `ChainOriginDirectory` consumes to re-close the authorized-origin gate for
    /// a fresh backend-only hash (#1373).
    pub async fn remove_origin(
        &self,
        owner: &PrivateKeySigner,
        namespace: U256,
        operator: Address,
    ) -> anyhow::Result<()> {
        let provider = self.provider_for(owner);
        let receipt = OriginAssignment::new(self.addrs.origin_assignment, &provider)
            .removeOrigin(namespace, operator)
            .send()
            .await
            .context("removeOrigin send")?
            .get_receipt()
            .await
            .context("removeOrigin receipt")?;
        crate::ensure_mined(&receipt, "removeOrigin")
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

    /// Current `FeeRouter.bytesPerEpoch(operator)` for the epoch at the chain's
    /// head timestamp — the governance-canonical served-bytes counter (ADR 036).
    pub async fn served_bytes(&self, operator: Address) -> anyhow::Result<U256> {
        let block = self
            .admin
            .get_block(alloy::eips::BlockId::latest())
            .await
            .context("get latest block")?
            .ok_or_else(|| anyhow::anyhow!("no latest block"))?;
        let epoch = block.header.timestamp / EPOCH_LENGTH_SECS;
        let fee = crate::bindings::FeeRouter::new(self.addrs.fee_router, &self.admin);
        fee.bytesPerEpoch(operator, epoch)
            .call()
            .await
            .context("read bytesPerEpoch")
    }

    /// Current chain head `block.timestamp` (seconds). Also used to stamp
    /// slash evidence strictly after a blacklist entry's `addedAt`.
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
    /// blacklist entry becomes slashable).
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
    /// entry's `addedAt` and the moment it becomes slashable. Read rather than
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

    /// Read the `addedAt` second-timestamp of the `(region, hash)` entry (`0`
    /// means not blacklisted). Serving a response timestamped after this is
    /// slashable while the entry is live.
    pub async fn blacklist_added_at(&self, region: B256, hash: B256) -> anyhow::Result<u64> {
        let entry = ContentBlacklist::new(self.addrs.content_blacklist, &self.admin)
            .getHashEntry(region, hash)
            .call()
            .await
            .context("getHashEntry")?;
        Ok(entry.addedAt)
    }

    /// `CapacityBond.unbondingOf(operator)` — the in-flight unbonding request
    /// as `(amount, unlockAt)`; `amount == 0` means none. A non-zero amount
    /// also makes `isActive` false for the whole window.
    pub async fn unbonding_of(&self, operator: Address) -> anyhow::Result<(U256, u64)> {
        let req = CapacityBond::new(self.addrs.capacity_bond, &self.admin)
            .unbondingOf(operator)
            .call()
            .await
            .context("read unbondingOf")?;
        Ok((req.amount, u64::try_from(req.unlockAt).unwrap_or(u64::MAX)))
    }

    /// Governable `CapacityBond.unbondingPeriod` (seconds; 14 days by
    /// default). Read so unbond journeys warp by the live value rather than
    /// hardcoding the default.
    pub async fn unbonding_period(&self) -> anyhow::Result<u64> {
        let secs = CapacityBond::new(self.addrs.capacity_bond, &self.admin)
            .unbondingPeriod()
            .call()
            .await
            .context("read unbondingPeriod")?;
        Ok(u64::try_from(secs).unwrap_or(u64::MAX))
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

    /// Governable `SlashAppeal.appealBond` (TOKEN base units).
    pub async fn appeal_bond(&self) -> anyhow::Result<U256> {
        SlashAppeal::new(self.addrs.slash_appeal, &self.admin)
            .appealBond()
            .call()
            .await
            .context("read appealBond")
    }

    /// `CapacityBond.slashedAtEpoch(operator)` — the ADR-036 vote-weight
    /// watermark (0 = no standing slash).
    pub async fn slashed_at_epoch(&self, operator: Address) -> anyhow::Result<u64> {
        CapacityBond::new(self.addrs.capacity_bond, &self.admin)
            .slashedAtEpoch(operator)
            .call()
            .await
            .context("read slashedAtEpoch")
    }

    /// `SlashAppeal` status for `slash_id` (0=None, 1=Open, 2=FastTracked,
    /// 3=Resolved).
    pub async fn appeal_status(&self, slash_id: U256) -> anyhow::Result<u8> {
        let appeal = SlashAppeal::new(self.addrs.slash_appeal, &self.admin)
            .getAppeal(slash_id)
            .call()
            .await
            .context("read getAppeal")?;
        Ok(appeal.status as u8)
    }

    /// TOKEN balance of `who` (base units).
    pub async fn token_balance(&self, who: Address) -> anyhow::Result<U256> {
        Erc20::new(self.addrs.token, &self.admin)
            .balanceOf(who)
            .call()
            .await
            .context("read TOKEN balance")
    }

    /// Slash `operator` through a real `SlashJudge` rate-manipulation
    /// commit-reveal challenge (#1032, G-NODE-05). Builds the operator's own
    /// self-incriminating probe (`hasBlob=true`, low rate) + stream (`ok=true`,
    /// higher rate) evidence for the same `blob_hash`, signs it with the
    /// operator's eth key over the `SlashJudge` EIP-712 domain, commits, warps
    /// past `MIN_REVEAL_DELAY`, and reveals as `challenger`. Returns the minted
    /// `slashId`.
    // The commit-reveal flow (arm challenger → build+sign evidence → commit →
    // warp → reveal → extract slashId) reads linearly; splitting it would
    // scatter the evidence construction across helpers.
    #[allow(clippy::too_many_lines)]
    pub async fn slash_operator_via_judge(
        &self,
        challenger: &PrivateKeySigner,
        operator: &PrivateKeySigner,
        node_id: B256,
        blob_hash: B256,
    ) -> anyhow::Result<U256> {
        self.arm_challenger(challenger).await?;
        let ch_provider = self.provider_for(challenger);

        // Evidence timestamps anchored just behind the current chain time
        // (within the 5-day age bound and the 30s probe↔stream window).
        let now = self.head_timestamp().await?;
        let probe_ts_us = (now - 10) * 1_000_000;
        let stream_ts_us = (now - 5) * 1_000_000;
        // Rate manipulation: probe quotes `probe_rate`, delivery charges a
        // higher `stream_rate` for the same hash inside the 30s window.
        let probe_rate: u64 = 10;
        let stream_rate: u64 = 25;
        let total_bytes: u64 = 1_048_576;
        let channel_id = B256::from(U256::from(1u64));

        let probe = ProbeSlashData {
            hash: blob_hash,
            has_blob: true,
            rate_per_mb: probe_rate,
            timestamp_us: probe_ts_us,
        };
        let stream = StreamSlashData {
            hash: blob_hash,
            ok: true,
            rate_per_mb: stream_rate,
            total_bytes,
            channel_id,
            timestamp_us: stream_ts_us,
            redirect: B256::ZERO,
        };
        let domain = slash_judge_domain(self.chain_id, self.addrs.slash_judge);
        let probe_sig = probe
            .sign(operator, &domain)
            .context("sign probe evidence")?
            .as_bytes()
            .to_vec();
        let stream_sig = stream
            .sign(operator, &domain)
            .context("sign stream evidence")?
            .as_bytes()
            .to_vec();

        // evidenceHash = keccak256(abi.encode(uint8(RateManipulation),
        // probeStructHash, streamStructHash)); commitment binds it to (salt,
        // challenger). `abi.encode(uint8 v)` right-aligns `v` in a 32-byte word
        // — byte-identical to `uint256(v)`, which alloy's `SolValue` encodes.
        let offense = U256::from(Offense::RateManipulation.discriminant());
        let evidence_hash =
            keccak256((offense, probe.struct_hash(), stream.struct_hash()).abi_encode());
        let salt = B256::repeat_byte(0x99);
        let commitment = keccak256((evidence_hash, salt, challenger.address()).abi_encode());

        let judge = SlashJudge::new(self.addrs.slash_judge, &ch_provider);
        let commit = judge
            .commitChallenge(commitment)
            .send()
            .await
            .context("commitChallenge send")?
            .get_receipt()
            .await
            .context("commitChallenge receipt")?;
        crate::ensure_mined(&commit, "commitChallenge")?;

        // Mature the commitment past MIN_REVEAL_DELAY (1 minute) with margin.
        crate::time::increase_time(&self.admin, 65).await?;

        let probe_msg = SlashJudge::ProbeMsg {
            hash: blob_hash,
            hasBlob: true,
            ratePerMb: probe_rate,
            timestampUs: probe_ts_us,
        };
        let stream_msg = SlashJudge::StreamMsg {
            hash: blob_hash,
            ok: true,
            ratePerMb: stream_rate,
            totalBytes: total_bytes,
            channelId: channel_id,
            timestampUs: stream_ts_us,
            redirect: B256::ZERO,
        };
        let receipt = crate::bindings::SlashJudgeRate::new(self.addrs.slash_judge, &ch_provider)
            .submitRateChallenge(
                operator.address(),
                node_id,
                Bytes::from(probe_msg.abi_encode()),
                Bytes::from(probe_sig),
                Bytes::from(stream_msg.abi_encode()),
                Bytes::from(stream_sig),
                salt,
            )
            .send()
            .await
            .context("submitRateChallenge send")?
            .get_receipt()
            .await
            .context("submitRateChallenge receipt")?;
        crate::ensure_mined(&receipt, "submitRateChallenge")?;

        self.extract_slash_id(
            receipt.inner.logs(),
            "submitRateChallenge",
            Some((Offense::RateManipulation, evidence_hash)),
        )
    }

    /// Extract the `SlashJudge.Slashed` `slashId` from a reveal receipt's logs,
    /// failing loudly on the pathologies an inline scan hides:
    ///
    ///  * a `Slashed` log emitted by `SlashJudge` whose body fails to decode is
    ///    surfaced as the ABI mismatch it is, rather than swallowed by an
    ///    `if let Ok(..)` that bails "emitted no Slashed event" and points at the
    ///    contract when the cause is a stale hand-written binding;
    ///  * when `expected` is supplied, the decoded `offenseType` and
    ///    `evidenceHash` must match, pinning "slashed for the reason we induced"
    ///    inside the fixture instead of leaving it to each journey.
    ///
    /// `CapacityBond.Slashed` is emitted in the same tx but under a different
    /// address+topic0, so the filter never confuses the two.
    fn extract_slash_id(
        &self,
        logs: &[alloy::rpc::types::Log],
        what: &str,
        expected: Option<(Offense, B256)>,
    ) -> anyhow::Result<U256> {
        for log in logs {
            if log.address() != self.addrs.slash_judge
                || log.topic0() != Some(&SlashJudge::Slashed::SIGNATURE_HASH)
            {
                continue;
            }
            let ev = SlashJudge::Slashed::decode_log_data(&log.inner.data).with_context(|| {
                format!("{what}: a SlashJudge Slashed log failed to decode (ABI drift?)")
            })?;
            if let Some((offense, evidence_hash)) = expected {
                // `OffenseType` (a generated `sol!` enum) derives neither
                // `PartialEq` nor `Debug`, so compare the canonical u8
                // discriminants — the same encoding folded into `evidenceHash`.
                let got = ev.offenseType as u8;
                let want = offense.discriminant();
                anyhow::ensure!(
                    got == want,
                    "{what}: slashed for offenseType {got}, but induced {want}",
                );
                anyhow::ensure!(
                    ev.evidenceHash == evidence_hash,
                    "{what}: slashed on a different evidenceHash than the one induced",
                );
            }
            return Ok(ev.slashId);
        }
        anyhow::bail!("{what} mined but emitted no SlashJudge Slashed event")
    }

    /// Governable `SlashJudge.challengeBond` (TOKEN base units) — pulled and
    /// returned inside a successful reveal, and per ADR 014 § Bond Handling never
    /// transferred at all when verification fails.
    pub async fn challenge_bond(&self) -> anyhow::Result<U256> {
        SlashJudge::new(self.addrs.slash_judge, &self.admin)
            .challengeBond()
            .call()
            .await
            .context("read challengeBond")
    }

    /// Fund `challenger` with gas + the `SlashJudge.challengeBond`, approved to
    /// the judge, and return the bond amount. Any funded EOA may challenge; the
    /// bond is pulled and returned inside the same reveal transaction.
    async fn arm_challenger(&self, challenger: &PrivateKeySigner) -> anyhow::Result<U256> {
        self.fund_eth(challenger.address(), 10).await?;
        let bond = SlashJudge::new(self.addrs.slash_judge, &self.admin)
            .challengeBond()
            .call()
            .await
            .context("read challengeBond")?;
        self.transfer_token(challenger.address(), bond).await?;
        let approve = Erc20::new(self.addrs.token, &self.provider_for(challenger))
            .approve(self.addrs.slash_judge, bond)
            .send()
            .await
            .context("challenger token.approve send")?
            .get_receipt()
            .await
            .context("challenger token.approve receipt")?;
        crate::ensure_mined(&approve, "challenger token.approve")?;
        Ok(bond)
    }

    /// Drive a `SlashJudge` commit-reveal challenge from **caller-supplied**
    /// evidence — the real signed `ProbeResponse` / `StreamResponse` a live
    /// daemon produced (#1042, G-GOV-03).
    ///
    /// The sibling of [`Self::slash_operator_via_judge`], which synthesises its
    /// own evidence and signs it with the operator key the test happens to hold.
    /// That is exactly what `SlashJudge.t.sol` can already do; what it cannot do
    /// is prove the *daemon's* signing path emits court-admissible bytes. So this
    /// one takes the wire messages verbatim — including `slash_sig`, which it
    /// never re-signs — and is therefore also the entry point for the negatives
    /// (forged signature, out-of-window pair), which are just the same call with
    /// one field of the real evidence perturbed.
    ///
    /// Handles the `_verifyPair` offense [`Offense::RateManipulation`]
    /// (`stream.ok && stream.rate_per_mb > probe.rate_per_mb`). `salt` is
    /// caller-supplied so two challenges in one journey cannot collide on a
    /// commitment slot.
    ///
    /// Returns the minted `slashId`. An `Err` here is the *whole point* for the
    /// negative cases: a challenge that fails verification reverts, and per
    /// [ADR 014 § Bond Handling](../../../adr/014-on-chain-verification.md) the
    /// bond is never transferred on a failed verification — assert the
    /// challenger's TOKEN balance directly.
    ///
    /// **Side effect:** advances chain time ~65s on *every* call, including ones
    /// that revert, to mature the commitment past `MIN_REVEAL_DELAY`. A journey
    /// that captures evidence *between* challenges is working against a drifted
    /// clock; anchor every capture with a fresh [`Self::head_timestamp`].
    pub async fn challenge_with_real_evidence(
        &self,
        challenger: &PrivateKeySigner,
        operator: Address,
        node_id: B256,
        offense: Offense,
        evidence: EvidencePair<'_>,
        salt: B256,
    ) -> anyhow::Result<U256> {
        let EvidencePair { probe, stream } = evidence;
        let bond = self.arm_challenger(challenger).await?;
        anyhow::ensure!(bond > U256::ZERO, "challenge bond must be non-zero");
        let ch_provider = self.provider_for(challenger);

        // Rebuild the EIP-712 struct hashes from the wire bodies with the same
        // production signers the daemon used, so the commitment binds the exact
        // messages the reveal submits.
        let probe_data = ProbeSlashData {
            hash: B256::from(probe.body.hash),
            has_blob: probe.body.has_blob,
            rate_per_mb: probe.body.rate_per_mb,
            timestamp_us: probe.body.timestamp_us,
        };
        let stream_data = StreamSlashData::from_response_body(&stream.body);
        let evidence_hash = keccak256(
            (
                U256::from(offense.discriminant()),
                probe_data.struct_hash(),
                stream_data.struct_hash(),
            )
                .abi_encode(),
        );
        let commitment = keccak256((evidence_hash, salt, challenger.address()).abi_encode());

        let judge = SlashJudge::new(self.addrs.slash_judge, &ch_provider);
        let commit = judge
            .commitChallenge(commitment)
            .send()
            .await
            .context("commitChallenge send")?
            .get_receipt()
            .await
            .context("commitChallenge receipt")?;
        crate::ensure_mined(&commit, "commitChallenge")?;

        // Mature past MIN_REVEAL_DELAY (1 minute) with margin.
        crate::time::increase_time(&self.admin, 65).await?;

        let probe_msg = SlashJudge::ProbeMsg {
            hash: probe_data.hash,
            hasBlob: probe_data.has_blob,
            ratePerMb: probe_data.rate_per_mb,
            timestampUs: probe_data.timestamp_us,
        };
        let stream_msg = SlashJudge::StreamMsg {
            hash: stream_data.hash,
            ok: stream_data.ok,
            ratePerMb: stream_data.rate_per_mb,
            totalBytes: stream_data.total_bytes,
            channelId: stream_data.channel_id,
            timestampUs: stream_data.timestamp_us,
            redirect: stream_data.redirect,
        };
        // The daemon's own signatures, forwarded byte-for-byte.
        let probe_sig = Bytes::from(probe.slash_sig.clone());
        let stream_sig = Bytes::from(stream.slash_sig.clone());
        let probe_bytes = Bytes::from(probe_msg.abi_encode());
        let stream_bytes = Bytes::from(stream_msg.abi_encode());

        let pending = match offense {
            Offense::RateManipulation => {
                crate::bindings::SlashJudgeRate::new(self.addrs.slash_judge, &ch_provider)
                    .submitRateChallenge(
                        operator,
                        node_id,
                        probe_bytes,
                        probe_sig,
                        stream_bytes,
                        stream_sig,
                        salt,
                    )
                    .send()
                    .await
            }
        };
        let what = offense.entry_point();
        let receipt = pending
            .with_context(|| format!("{what} send"))?
            .get_receipt()
            .await
            .with_context(|| format!("{what} receipt"))?;
        crate::ensure_mined(&receipt, what)?;

        self.extract_slash_id(receipt.inner.logs(), what, Some((offense, evidence_hash)))
    }

    /// `CapacityBond.escrowedTotal()` — TOKEN currently parked in slash escrow.
    pub async fn escrowed_total(&self) -> anyhow::Result<U256> {
        CapacityBond::new(self.addrs.capacity_bond, &self.admin)
            .escrowedTotal()
            .call()
            .await
            .context("read escrowedTotal")
    }

    /// `CapacityBond.slashRecords(slashId)` → `(operator, slashedAt, slashAmount)`.
    pub async fn slash_record(&self, slash_id: U256) -> anyhow::Result<(Address, u64, U256)> {
        let rec = CapacityBond::new(self.addrs.capacity_bond, &self.admin)
            .slashRecords(slash_id)
            .call()
            .await
            .context("read slashRecords")?;
        Ok((rec.operator, rec.slashedAt, rec.slashAmount))
    }

    /// Permissionless `CapacityBond.finalizeUnappealedSlash` — distributes an
    /// escrowed slash whose 30-day filing window lapsed, 50% to the recorded
    /// challenger and 50% burned. Sent by the admin EOA (the caller is not part
    /// of the split; the challenger is read from the record).
    pub async fn finalize_unappealed_slash(&self, slash_id: U256) -> anyhow::Result<()> {
        let receipt = CapacityBond::new(self.addrs.capacity_bond, &self.admin)
            .finalizeUnappealedSlash(slash_id)
            .send()
            .await
            .context("finalizeUnappealedSlash send")?
            .get_receipt()
            .await
            .context("finalizeUnappealedSlash receipt")?;
        crate::ensure_mined(&receipt, "finalizeUnappealedSlash")
    }

    /// TOKEN `totalSupply()` — reads the burn leg of a 50/50 slash split, which
    /// no account's balance reflects.
    pub async fn token_total_supply(&self) -> anyhow::Result<U256> {
        Erc20::new(self.addrs.token, &self.admin)
            .totalSupply()
            .call()
            .await
            .context("read TOKEN totalSupply")
    }

    /// Emergency-multisig `fastTrackAppeal`. The e2e deploy sets
    /// `EMERGENCY_MULTISIG = DEPLOYER_ADDR` (anvil dev #0), so the deployer key
    /// holds the role.
    pub async fn fast_track_appeal(&self, slash_id: U256) -> anyhow::Result<()> {
        let deployer: PrivateKeySigner = DEPLOYER_KEY.parse().context("parse deployer key")?;
        let provider = self.provider_for(&deployer);
        let receipt = SlashAppeal::new(self.addrs.slash_appeal, &provider)
            .fastTrackAppeal(slash_id)
            .send()
            .await
            .context("fastTrackAppeal send")?
            .get_receipt()
            .await
            .context("fastTrackAppeal receipt")?;
        crate::ensure_mined(&receipt, "fastTrackAppeal")
    }

    /// Grant a fast-tracked appeal through a real Governor proposal:
    /// propose → (warp votingDelay) → castVote(For) → (warp votingPeriod) →
    /// queue → (warp timelock minDelay) → execute, where the executed action is
    /// `SlashAppeal.grantAppeal(slash_id)`. `voter` must already hold nonzero
    /// vote weight at the proposal snapshot (ADR-036 served bytes × age ramp).
    pub async fn governor_grant_appeal(
        &self,
        slash_id: U256,
        voter: &PrivateKeySigner,
    ) -> anyhow::Result<()> {
        self.fund_eth(voter.address(), 10).await?;
        let vp = self.provider_for(voter);
        let gov = DecdnGovernor::new(self.addrs.governor, &vp);

        // Build the grantAppeal call the Timelock will execute.
        let appeal = SlashAppeal::new(self.addrs.slash_appeal, &vp);
        let calldata = appeal.grantAppeal(slash_id).calldata().clone();
        let targets = vec![self.addrs.slash_appeal];
        let values = vec![U256::ZERO];
        let calldatas = vec![calldata];
        let description = format!("grant slash appeal {slash_id}");
        let desc_hash = keccak256(description.as_bytes());

        // Static call returns the proposalId the send will mint (deterministic
        // hashProposal); trust it only after the send is mined.
        let proposal_id = gov
            .propose(
                targets.clone(),
                values.clone(),
                calldatas.clone(),
                description.clone(),
            )
            .call()
            .await
            .context("propose static call")?;
        let propose = gov
            .propose(
                targets.clone(),
                values.clone(),
                calldatas.clone(),
                description,
            )
            .send()
            .await
            .context("propose send")?
            .get_receipt()
            .await
            .context("propose receipt")?;
        crate::ensure_mined(&propose, "propose")?;

        let voting_delay = gov.votingDelay().call().await.context("read votingDelay")?;
        crate::time::increase_time(&self.admin, voting_delay.to::<u64>() + 2).await?;

        let vote = gov
            .castVote(proposal_id, 1) // 1 = For
            .send()
            .await
            .context("castVote send")?
            .get_receipt()
            .await
            .context("castVote receipt")?;
        crate::ensure_mined(&vote, "castVote")?;

        let voting_period = gov
            .votingPeriod()
            .call()
            .await
            .context("read votingPeriod")?;
        crate::time::increase_time(&self.admin, voting_period.to::<u64>() + 2).await?;

        let queue = gov
            .queue(
                targets.clone(),
                values.clone(),
                calldatas.clone(),
                desc_hash,
            )
            .send()
            .await
            .context("queue send")?
            .get_receipt()
            .await
            .context("queue receipt")?;
        crate::ensure_mined(&queue, "queue")?;

        let min_delay = TimelockController::new(self.addrs.timelock, &vp)
            .getMinDelay()
            .call()
            .await
            .context("read timelock minDelay")?;
        crate::time::increase_time(&self.admin, min_delay.to::<u64>() + 2).await?;

        let execute = gov
            .execute(targets, values, calldatas, desc_hash)
            .send()
            .await
            .context("execute send")?
            .get_receipt()
            .await
            .context("execute receipt")?;
        crate::ensure_mined(&execute, "execute")
    }
}

/// `crates/e2e/ → ../../contracts`.
fn contracts_dir() -> anyhow::Result<PathBuf> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
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
    let code_hex = artifact_bytecode(&contracts.join("out/MintableUSDC.sol/MintableUSDC.json"))?;
    let code: Bytes = code_hex.parse().context("parse MintableUSDC bytecode")?;
    let receipt = provider
        .send_transaction(TransactionRequest::default().with_deploy_code(code))
        .await
        .context("MintableUSDC deploy send")?
        .get_receipt()
        .await
        .context("MintableUSDC deploy receipt")?;
    receipt
        .contract_address
        .ok_or_else(|| anyhow::anyhow!("MintableUSDC deploy produced no contract address"))
}

/// A one-time protocol deployment shared by every journey in a test run: the
/// path to the anvil state snapshot to replay, plus the addresses read back from
/// the deploy manifest. Produced by [`ensure_shared_deployment`].
struct SharedDeployment {
    state_path: PathBuf,
    addrs: ContractAddrs,
    usdc: Address,
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
    let state_path = dir.join(format!("state-{key}.json"));
    let manifest_path = dir.join(format!("manifest-{key}.json"));

    if let Some(shared) = load_cached_deployment(&state_path, &manifest_path)? {
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
    let shared = match load_cached_deployment(&state_path, &manifest_path)? {
        Some(shared) => shared,
        None => deploy_and_snapshot(contracts, &state_path, &manifest_path).await?,
    };
    drop(lock);
    Ok(shared)
}

/// Run the one-time deploy in a throwaway anvil and commit its state snapshot +
/// manifest to the cache. The caller holds the bootstrap lock for the duration.
async fn deploy_and_snapshot(
    contracts: &Path,
    state_path: &Path,
    manifest_path: &Path,
) -> anyhow::Result<SharedDeployment> {
    let forge_manifest = contracts.join(format!("deployments/{E2E_CHAIN_ID}.json"));
    // The guard removes the forge manifest on drop; the cache keeps its own copy.
    let anvil = spawn_anvil(Some(forge_manifest.clone())).await?;

    // Mock USDC first, then the protocol with the initial TOKEN supply held by
    // the admin EOA so it can distribute bond stake to N operators.
    let usdc = deploy_mock_usdc(&anvil.admin, contracts).await?;
    let token_holder = admin_address()?;
    run_deploy_script(&anvil.admin, contracts, &anvil.rpc_url, usdc, token_holder).await?;

    let (addrs, manifest_usdc) = read_manifest(&forge_manifest)?;
    anyhow::ensure!(
        manifest_usdc == usdc,
        "manifest externalDeps.usdc {manifest_usdc} disagrees with deployed mock USDC {usdc}"
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
    write_atomic(state_path, state_hex.as_bytes())?;
    let manifest_bytes = std::fs::read(&forge_manifest)
        .with_context(|| format!("re-read forge manifest {}", forge_manifest.display()))?;
    write_atomic(manifest_path, &manifest_bytes)?;

    Ok(SharedDeployment {
        state_path: state_path.to_path_buf(),
        addrs,
        usdc,
    })
}

/// Load a previously committed deployment, or `None` if the cache is absent or
/// incomplete (the manifest is the commit marker, but require both files so a
/// torn write is never mistaken for a hit).
fn load_cached_deployment(
    state_path: &Path,
    manifest_path: &Path,
) -> anyhow::Result<Option<SharedDeployment>> {
    if !nonempty_file(state_path) || !nonempty_file(manifest_path) {
        return Ok(None);
    }
    let (addrs, usdc) = read_manifest(manifest_path)?;
    Ok(Some(SharedDeployment {
        state_path: state_path.to_path_buf(),
        addrs,
        usdc,
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
    const SCHEMA: &str = "v1";
    let usdc_bc = artifact_bytecode(&contracts.join("out/MintableUSDC.sol/MintableUSDC.json"))?;
    let deploy_bc =
        artifact_bytecode(&contracts.join("out/DeployProtocol.s.sol/DeployProtocol.json"))?;
    let mut buf = Vec::new();
    buf.extend_from_slice(SCHEMA.as_bytes());
    buf.extend_from_slice(&E2E_CHAIN_ID.to_le_bytes());
    buf.extend_from_slice(usdc_bc.as_bytes());
    buf.extend_from_slice(deploy_bc.as_bytes());
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
    usdc: Address,
    initial_token_holder: Address,
) -> anyhow::Result<()> {
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

/// The global-scope region key: `bytes32("GLOBAL")`.
#[must_use]
pub fn global_region() -> B256 {
    B256::right_padding_from(b"GLOBAL")
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

/// Read the protocol contract addresses and the settlement USDC from the deploy
/// manifest. The mock USDC is recorded under `externalDeps.usdc` — the same
/// address the fixture deployed and fed to the script as `USDC_ADDRESS`.
fn read_manifest(path: &Path) -> anyhow::Result<(ContractAddrs, Address)> {
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
    let addrs = ContractAddrs {
        capacity_bond: get("CapacityBond")?,
        payment_pool: get("PaymentPool")?,
        fee_router: get("FeeRouter")?,
        token: get("Token")?,
        slash_judge: get("SlashJudge")?,
        slash_appeal: get("SlashAppeal")?,
        governor: get("DecdnGovernor")?,
        timelock: get("TimelockController")?,
        publisher_registry: get("PublisherRegistry")?,
        origin_assignment: get("OriginAssignment")?,
        manual_vetting_policy: get("ManualVettingPolicy")?,
        content_blacklist: get("ContentBlacklist")?,
    };
    let usdc: Address = json
        .get("externalDeps")
        .and_then(|d| d.get("usdc"))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("manifest missing externalDeps.usdc"))?
        .parse()
        .context("parse manifest externalDeps.usdc")?;
    Ok((addrs, usdc))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forge_body_completion_gates_on_the_success_marker() {
        // A non-zero exit *after* forge prints this marker is a broadcast-phase
        // hiccup (retryable, #883); its absence means a revert before the body
        // ran (fail fast). The classifier keys purely off the marker string, so
        // pin both branches — if forge changes the wording this test catches it.
        assert!(forge_script_body_completed(
            b"...\nScript ran successfully.\n== Logs ==\n"
        ));
        assert!(!forge_script_body_completed(
            b"Error: script failed: revert: Ownable: caller is not the owner"
        ));
        assert!(!forge_script_body_completed(b""));
    }

    // The shared-deployment cache helpers are pure (no anvil/forge), so pin their
    // correctness here rather than only through the gated journeys.
    #[allow(clippy::expect_used, clippy::panic)]
    fn write_json(path: &Path, json: &str) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create parent dir");
        }
        std::fs::write(path, json).expect("write json fixture");
    }

    /// A minimal deploy manifest with distinct, valid addresses for each field.
    fn sample_manifest_json() -> String {
        let addr = |n: u8| format!("0x{n:0>40x}");
        format!(
            r#"{{
              "contracts": {{
                "CapacityBond": "{}", "PaymentPool": "{}", "FeeRouter": "{}",
                "Token": "{}", "SlashJudge": "{}", "SlashAppeal": "{}",
                "DecdnGovernor": "{}", "TimelockController": "{}",
                "PublisherRegistry": "{}", "OriginAssignment": "{}",
                "ManualVettingPolicy": "{}", "ContentBlacklist": "{}"
              }},
              "externalDeps": {{ "usdc": "{}" }}
            }}"#,
            addr(1),
            addr(2),
            addr(3),
            addr(4),
            addr(5),
            addr(6),
            addr(7),
            addr(8),
            addr(9),
            addr(10),
            addr(11),
            addr(12),
            addr(0xff),
        )
    }

    #[test]
    #[allow(clippy::expect_used, clippy::panic)]
    fn short_hash_is_stable_distinct_and_hex() {
        let a = short_hash(b"alpha");
        assert_eq!(a, short_hash(b"alpha"), "same input, same hash");
        assert_ne!(a, short_hash(b"beta"), "different input, different hash");
        assert_eq!(a.len(), 16, "8 bytes as hex");
        assert!(
            a.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
    }

    #[test]
    #[allow(clippy::expect_used, clippy::panic)]
    fn write_atomic_commits_whole_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("state.json");
        assert!(!nonempty_file(&path), "absent before write");
        write_atomic(&path, b"0xdeadbeef").expect("atomic write");
        assert!(nonempty_file(&path), "present after write");
        assert_eq!(std::fs::read(&path).expect("read back"), b"0xdeadbeef");
        // No sibling temp file is left behind.
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .expect("read dir")
            .filter_map(Result::ok)
            .filter(|e| e.file_name() != "state.json")
            .collect();
        assert!(
            leftovers.is_empty(),
            "temp file renamed away, found {leftovers:?}"
        );
    }

    #[test]
    #[allow(clippy::expect_used, clippy::panic)]
    fn read_manifest_parses_contracts_and_usdc() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("manifest.json");
        write_json(&path, &sample_manifest_json());
        let (addrs, usdc) = read_manifest(&path).expect("parse manifest");
        assert_eq!(addrs.capacity_bond, Address::with_last_byte(1));
        assert_eq!(addrs.content_blacklist, Address::with_last_byte(12));
        assert_eq!(usdc, Address::with_last_byte(0xff));
    }

    #[test]
    #[allow(clippy::expect_used, clippy::panic)]
    fn read_manifest_requires_usdc() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("manifest.json");
        // Strip externalDeps: the settlement token must be present.
        write_json(&path, r#"{ "contracts": {} }"#);
        assert!(read_manifest(&path).is_err());
    }

    #[test]
    #[allow(clippy::expect_used, clippy::panic)]
    fn load_cached_deployment_needs_both_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = dir.path().join("state.json");
        let manifest = dir.path().join("manifest.json");

        // Neither present, or only one present, is a miss.
        assert!(
            load_cached_deployment(&state, &manifest)
                .expect("miss")
                .is_none()
        );
        write_atomic(&state, b"0x00").expect("write state");
        assert!(
            load_cached_deployment(&state, &manifest)
                .expect("half-written miss")
                .is_none(),
            "manifest is the commit marker; state alone is not a hit"
        );

        // Both present is a hit that surfaces the parsed addresses.
        write_json(&manifest, &sample_manifest_json());
        let hit = load_cached_deployment(&state, &manifest)
            .expect("hit")
            .expect("some");
        assert_eq!(hit.usdc, Address::with_last_byte(0xff));
        assert_eq!(hit.state_path, state);
    }

    #[test]
    #[allow(clippy::expect_used, clippy::panic)]
    fn artifact_cache_key_invalidates_on_bytecode_change() {
        let dir = tempfile::tempdir().expect("tempdir");
        let contracts = dir.path();
        let usdc = contracts.join("out/MintableUSDC.sol/MintableUSDC.json");
        let deploy = contracts.join("out/DeployProtocol.s.sol/DeployProtocol.json");
        write_json(&usdc, r#"{ "bytecode": { "object": "0x6001" } }"#);
        write_json(&deploy, r#"{ "bytecode": { "object": "0x6002" } }"#);

        let key1 = artifact_cache_key(contracts).expect("key1");
        assert_eq!(
            key1,
            artifact_cache_key(contracts).expect("key1 again"),
            "deterministic"
        );

        // A recompiled deploy script (embeds every `new`d contract) rekeys the cache.
        write_json(&deploy, r#"{ "bytecode": { "object": "0x6003" } }"#);
        assert_ne!(key1, artifact_cache_key(contracts).expect("key2"));
    }

    #[test]
    fn cache_dir_is_per_checkout() {
        assert_ne!(
            cache_dir(Path::new("/a/contracts")),
            cache_dir(Path::new("/b/contracts"))
        );
    }

    /// Regression test for the deploy-retry death spiral (#785).
    ///
    /// Models the state a SIGKILLed `forge script` leaves behind: a transaction
    /// already on the wire but *not yet mined*. Before the drain, `evm_revert`
    /// left that straggler in the pool, so the deployer nonce read clean at
    /// snapshot time and then advanced once the straggler mined — poisoning the
    /// fresh snapshot and making every later attempt fail fast with `nonce too
    /// low` / `transaction already imported`.
    ///
    /// anvil runs on a fixed block time so the straggler is *provably* still
    /// pending while the lane is reset. That is the deterministic stand-in for
    /// the CI condition: a starved anvil that has not drained its pool by the
    /// time the stalled forge is SIGKILLed. The block time must exceed the drain
    /// budget, or the straggler mines before the revert and the revert alone
    /// would clean it up — which is precisely the case that never reproduced.
    #[cfg(feature = "anvil-e2e")]
    #[tokio::test]
    // Test scaffolding legitimately uses expect/panic; the workspace anti-panic
    // policy targets runtime code (matches the journey files' crate-level allow).
    #[allow(clippy::expect_used, clippy::panic)]
    async fn reset_broadcast_lane_survives_a_straggler_from_a_killed_forge() {
        // Must stay comfortably above the drain budget (POOL_DRAIN_POLLS *
        // POOL_DRAIN_SETTLE) so the straggler is still pending through the reset.
        const BLOCK_TIME: u64 = 4;

        let port = crate::free_port().expect("free port");
        let mut anvil = Command::new("anvil")
            .args([
                "--port",
                &port.to_string(),
                "--block-time",
                &BLOCK_TIME.to_string(),
                "--silent",
            ])
            .spawn()
            .expect("spawn anvil (is foundry installed?)");

        let url: reqwest::Url = format!("http://127.0.0.1:{port}")
            .parse()
            .expect("parse anvil rpc url");
        let deployer: PrivateKeySigner = DEPLOYER_KEY.parse().expect("parse deployer key");
        let deployer_addr = deployer.address();
        let provider: DynProvider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(deployer))
            .connect_http(url)
            .erased();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        while provider.get_chain_id().await.is_err() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "anvil RPC never came up"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let snapshot = evm_snapshot(&provider).await.expect("snapshot");
        let before = provider
            .get_transaction_count(deployer_addr)
            .await
            .expect("nonce before");

        // Submit without awaiting the receipt: on a fixed block time this leaves
        // the transaction sitting in the pool — exactly what a SIGKILLed forge
        // leaves behind.
        let pending = provider
            .send_transaction(
                TransactionRequest::default()
                    .with_to(Address::ZERO)
                    .with_value(U256::from(1)),
            )
            .await
            .expect("submit straggler");
        drop(pending);
        assert!(
            !pool_is_empty(&provider).await.expect("pool status"),
            "straggler was mined before the reset — the test is not exercising the race"
        );

        let _fresh = reset_broadcast_lane(&provider, snapshot)
            .await
            .expect("reset broadcast lane");

        // Past the next block: if the straggler survived the reset it has mined
        // by now and the nonce has moved past the fresh snapshot.
        tokio::time::sleep(Duration::from_secs(BLOCK_TIME * 2)).await;
        let after = provider
            .get_transaction_count(deployer_addr)
            .await
            .expect("nonce after");
        assert_eq!(
            after, before,
            "a straggler from the killed forge advanced the deployer nonce past the \
             fresh snapshot — every retry will now die with `nonce too low`"
        );
        assert!(
            pool_is_empty(&provider).await.expect("pool status"),
            "anvil's pool still holds transactions after the lane reset"
        );

        let _ = anvil.kill();
        let _ = anvil.wait();
    }

    #[test]
    fn ci_scaling_widens_the_budget_only_under_ci() {
        // Local runs keep the tighter base budget so a genuine hang fails fast;
        // CI multiplies it to absorb runner contention (#1384).
        let base = Duration::from_secs(60);
        assert_eq!(scale_for_ci(base, false), base);
        assert_eq!(scale_for_ci(base, true), base * CI_TIMEOUT_MULTIPLIER);
    }
}
