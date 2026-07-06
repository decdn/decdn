//! Chain fixture: spin up `anvil`, deploy the full protocol via the production
//! `DeployProtocol` forge script, and expose typed `alloy` handles + the helper
//! verbs (fund gas, mint USDC/TOKEN, onboard an operator, read served bytes)
//! the cross-layer journeys need.
//!
//! Generalizes the self-contained bring-up in
//! `crates/node/tests/anvil_settlement_e2e.rs` into a reusable fixture. Like
//! that test it shells out to `forge`/`anvil` (no extra crate deps) and so
//! requires both on `PATH`; the fixture is only reached behind the harness's
//! `anvil-e2e` gate.

use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::Duration;

use alloy::network::{EthereumWallet, TransactionBuilder};
use alloy::primitives::{Address, B256, Bytes, U256};
use alloy::providers::{DynProvider, Provider, ProviderBuilder};
use alloy::rpc::types::TransactionRequest;
use alloy::signers::SignerSync;
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use decdn_incentive::{bind_node_id_domain, binding_signing_hash, node_register};

use crate::bindings::{CapacityBond, Erc20, PublisherRegistry};

/// Base for the per-fixture chain id. Each `ChainFixture` derives its chain id
/// as `CHAIN_BASE + port` (the full ephemeral port), so concurrent fixtures (and
/// the node crate's `anvil_settlement_e2e.rs`) never share — and thus never race
/// on — the `deployments/<chain_id>.json` manifest. Because the OS never hands
/// the same port to two live listeners, a chain-id collision can only coincide
/// with a port collision, which already fails the anvil bind; folding the port
/// modulo a range (as an earlier revision did) instead *added* collisions. The
/// resulting `31_337_691_024..=31_337_755_535` range matches the
/// `deployments/31337[67]*.json` gitignore glob (`contracts/.gitignore`), so a
/// crashed run's leftover manifest stays untracked.
const CHAIN_BASE: u64 = 31_337_690_000;

/// Anvil dev account #0 — funded at genesis, broadcasts the deploy script.
const DEPLOYER_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
const DEPLOYER_ADDR: &str = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266";
/// Anvil dev account #1 — the "admin" EOA: deploys the mock USDC, holds the
/// initial TOKEN supply, and mints/transfers.
/// Deliberately NOT account #0 (the forge-script broadcaster, whose nonce the
/// script advances by ~25 — sharing it desyncs alloy's cached nonce).
const ADMIN_KEY: &str = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d";
const ADMIN_ADDR: &str = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8";

/// Deploy default `minBond` (50_000e18 TOKEN).
const MIN_BOND_WEI: &str = "50000000000000000000000";

/// `FeeRouter`/`CapacityBond` epoch length, for the served-bytes read.
const EPOCH_LENGTH_SECS: u64 = 7 * 24 * 60 * 60;

const FORGE_BUILD_TIMEOUT: Duration = Duration::from_secs(180);
const DEPLOY_TIMEOUT: Duration = Duration::from_secs(45);
const DEPLOY_ATTEMPTS: usize = 3;

/// Deployed protocol contract addresses, read from the forge-script manifest.
#[derive(Debug, Clone, Copy)]
pub struct ContractAddrs {
    pub capacity_bond: Address,
    pub payment_channel: Address,
    pub fee_router: Address,
    pub token: Address,
    pub slash_judge: Address,
    pub publisher_registry: Address,
    pub origin_assignment: Address,
    pub content_blacklist: Address,
}

/// Kills the spawned `anvil` on drop and removes the (gitignored) manifest so
/// re-runs start clean — a panicking assertion never leaks the process.
#[derive(Debug)]
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

/// A live anvil deployment of the full protocol with typed handles.
#[derive(Debug)]
pub struct ChainFixture {
    _anvil: AnvilGuard,
    /// JSON-RPC endpoint (e.g. `http://127.0.0.1:PORT`).
    pub rpc_url: String,
    /// Parsed RPC URL for building providers.
    pub url: reqwest::Url,
    /// Per-fixture chain id (anvil `--chain-id`); also the EIP-712 / ed25519
    /// domain chain id every signature in this fixture is bound to.
    pub chain_id: u64,
    /// Deployed contract addresses.
    pub addrs: ContractAddrs,
    /// Mock USDC (mintable) the settlement token points at.
    pub usdc: Address,
    /// Admin provider (anvil dev #1): raw RPC, minting, TOKEN distribution.
    pub admin: DynProvider,
    /// The admin EOA address (initial TOKEN holder, mock-USDC minter).
    pub admin_addr: Address,
}

impl ChainFixture {
    /// Build contracts, spawn anvil, deploy mock USDC + the full protocol, and
    /// return the fixture with typed handles. Requires `anvil` + `forge` on
    /// `PATH`.
    pub async fn launch() -> anyhow::Result<Self> {
        let contracts = contracts_dir()?;
        forge_build(&contracts).await?;

        let port = crate::free_port()?;
        // Unique per fixture so the deploy manifest path never collides with a
        // concurrent fixture's: the full ephemeral port is unique across live
        // listeners (see `CHAIN_BASE`).
        let chain_id = CHAIN_BASE + u64::from(port);
        let rpc_url = format!("http://127.0.0.1:{port}");
        let child = Command::new("anvil")
            .args([
                "--port",
                &port.to_string(),
                "--chain-id",
                &chain_id.to_string(),
                "--silent",
            ])
            .spawn()
            .context("spawn anvil (is foundry installed?)")?;
        let manifest = contracts.join(format!("deployments/{chain_id}.json"));
        let mut anvil = AnvilGuard {
            child,
            manifest: manifest.clone(),
        };

        let url: reqwest::Url = rpc_url.parse().context("parse anvil rpc url")?;
        let admin_signer: PrivateKeySigner = ADMIN_KEY.parse().context("parse admin key")?;
        let admin_addr = admin_signer.address();
        let admin: DynProvider = ProviderBuilder::new()
            .with_simple_nonce_management()
            .wallet(EthereumWallet::from(admin_signer))
            .connect_http(url.clone())
            .erased();

        // Wait for the RPC to accept requests, failing fast if anvil died at
        // startup (bad args / port clash) rather than waiting out the timeout.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            if admin.get_chain_id().await.is_ok() {
                break;
            }
            if let Ok(Some(status)) = anvil.child.try_wait() {
                anyhow::bail!("anvil exited prematurely before its RPC came up: {status}");
            }
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!("anvil RPC never came up within 20s");
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }

        // Deploy mock USDC, then the protocol with the initial TOKEN supply held
        // by the admin EOA so it can distribute bond stake to N operators.
        let usdc = deploy_mock_usdc(&admin, &contracts).await?;
        let admin_token_holder: Address = ADMIN_ADDR.parse().context("parse admin addr")?;
        run_deploy_script(&contracts, &rpc_url, usdc, admin_token_holder).await?;
        let addrs = read_manifest(&manifest)?;

        Ok(Self {
            _anvil: anvil,
            rpc_url,
            url,
            chain_id,
            addrs,
            usdc,
            admin,
            admin_addr,
        })
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
        let node_id = B256::from_slice(node_secret.public().as_bytes());
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

        let domain = bind_node_id_domain(self.chain_id, self.addrs.capacity_bond);
        let bind_hash = binding_signing_hash(node_id, binding_nonce, &domain);
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
                Bytes::from(binding_sig),
                Bytes::from(ed_sig),
            )
            .send()
            .await
            .context("registerNode send")?
            .get_receipt()
            .await
            .context("registerNode receipt")?;
        crate::ensure_mined(&register_receipt, "registerNode")?;

        anyhow::ensure!(
            bond.isActive(op_addr)
                .call()
                .await
                .context("read isActive")?,
            "operator must be active after bond + registerNode"
        );
        Ok(())
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
}

/// `crates/e2e/ → ../../contracts`.
fn contracts_dir() -> anyhow::Result<PathBuf> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../contracts")
        .canonicalize()
        .context("resolve contracts dir")
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
    let mut build_cmd = tokio::process::Command::new("forge");
    build_cmd.current_dir(contracts).args(["build"]);
    match forge_output(build_cmd, FORGE_BUILD_TIMEOUT, "forge build").await? {
        Ok(out) if out.status.success() => Ok(()),
        Ok(out) => anyhow::bail!(
            "forge build failed:\n{}\n{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        ),
        Err(timeout) => anyhow::bail!("`forge build` timed out after {timeout:?}"),
    }
}

/// Deploy the mintable mock USDC from its compiled artifact bytecode.
async fn deploy_mock_usdc<P: Provider>(provider: &P, contracts: &Path) -> anyhow::Result<Address> {
    let artifact = contracts.join("out/MintableUSDC.sol/MintableUSDC.json");
    let bytes = std::fs::read(&artifact)
        .with_context(|| format!("read MintableUSDC artifact at {}", artifact.display()))?;
    let json: serde_json::Value = serde_json::from_slice(&bytes)?;
    let code_hex = json
        .get("bytecode")
        .and_then(|b| b.get("object"))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("MintableUSDC artifact missing bytecode.object"))?;
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

/// Run `forge script DeployProtocol.s.sol` against the anvil RPC, retrying the
/// two transient failure classes (stall #785, broadcast-phase non-zero exit
/// #883) and failing fast on a deterministic revert.
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
            .env("CHALLENGER_INCENTIVE_POOL", DEPLOYER_ADDR)
            .env("FORCE_OVERWRITE_MANIFEST", "true");
        match forge_output(cmd, DEPLOY_TIMEOUT, "forge script DeployProtocol").await? {
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
    }
    anyhow::bail!("DEPLOY_ATTEMPTS must be >= 1 (was {DEPLOY_ATTEMPTS})")
}

/// Read the protocol contract addresses from the deploy manifest.
fn read_manifest(path: &Path) -> anyhow::Result<ContractAddrs> {
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
    Ok(ContractAddrs {
        capacity_bond: get("CapacityBond")?,
        payment_channel: get("PaymentChannel")?,
        fee_router: get("FeeRouter")?,
        token: get("Token")?,
        slash_judge: get("SlashJudge")?,
        publisher_registry: get("PublisherRegistry")?,
        origin_assignment: get("OriginAssignment")?,
        content_blacklist: get("ContentBlacklist")?,
    })
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
}
