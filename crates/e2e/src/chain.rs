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
    bind_node_id_domain, node_register, register_node_signing_hash, slash_judge_domain,
};

use crate::bindings::{
    AccessControl, CapacityBond, ContentBlacklist, DecdnGovernor, Erc20, PublisherRegistry,
    SlashAppeal, SlashJudge, TimelockController,
};

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

// A cold-CI compile of the full contracts suite can be slow; a single 180s cap
// with no retry made an over-budget-but-progressing build a hard failure. Give
// it headroom plus a bounded retry for a transient stall (mirrors the deploy
// retry). A real compile error still fails fast — only a timeout is retried.
const FORGE_BUILD_TIMEOUT: Duration = Duration::from_secs(300);
const BUILD_ATTEMPTS: usize = 3;
const DEPLOY_TIMEOUT: Duration = Duration::from_secs(45);
const DEPLOY_ATTEMPTS: usize = 3;
/// Re-pick the ephemeral port and re-spawn anvil this many times when it dies at
/// startup (the `free_port` TOCTOU: another process claimed the port first).
const ANVIL_ATTEMPTS: usize = 3;

/// Deployed protocol contract addresses, read from the forge-script manifest.
#[derive(Debug, Clone, Copy)]
pub struct ContractAddrs {
    pub capacity_bond: Address,
    pub payment_channel: Address,
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
///
/// The descriptive handles (endpoint, chain id, addresses, admin provider) are
/// facts about the already-running anvil process, fixed at [`Self::launch`].
/// They are exposed as accessors rather than `pub` fields so a caller can't
/// reassign one and silently desync the fixture from the live process it
/// describes (the endpoint used to be two `pub` fields, `rpc_url` + `url`, that
/// had to agree — now one stored form derives both).
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
    /// Build contracts, spawn anvil, deploy mock USDC + the full protocol, and
    /// return the fixture with typed handles. Requires `anvil` + `forge` on
    /// `PATH`.
    pub async fn launch() -> anyhow::Result<Self> {
        let contracts = contracts_dir()?;
        forge_build(&contracts).await?;

        let admin_signer: PrivateKeySigner = ADMIN_KEY.parse().context("parse admin key")?;
        let admin_addr = admin_signer.address();

        // Pick an ephemeral port and bring anvil up on it, re-picking on a
        // collision. `free_port` releases the port before anvil binds it, so
        // another process can take it in the gap (a TOCTOU that also seeds
        // `chain_id`); the collision makes anvil exit at startup, so re-pick and
        // retry rather than failing the whole fixture. The port is unique across
        // live listeners once claimed, so the deploy manifest path never
        // collides with a concurrent fixture's (see `CHAIN_BASE`).
        let mut attempt = 0;
        let (anvil, chain_id, rpc_url, url, admin) = loop {
            attempt += 1;
            let port = crate::free_port()?;
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
            let mut anvil = AnvilGuard { child, manifest };

            let url: reqwest::Url = rpc_url.parse().context("parse anvil rpc url")?;
            let admin: DynProvider = ProviderBuilder::new()
                .with_simple_nonce_management()
                .wallet(EthereumWallet::from(admin_signer.clone()))
                .connect_http(url.clone())
                .erased();

            // Wait for the RPC to accept requests. A premature anvil exit is
            // almost always the port clash above (retryable); a genuine timeout
            // is a hard failure.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
            let up = loop {
                if admin.get_chain_id().await.is_ok() {
                    break true;
                }
                if let Ok(Some(status)) = anvil.child.try_wait() {
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
                break (anvil, chain_id, rpc_url, url, admin);
            }
            // `anvil` (AnvilGuard) drops here: kills the dead child and removes
            // its manifest before the next attempt.
            if attempt >= ANVIL_ATTEMPTS {
                anyhow::bail!(
                    "anvil never became ready after {ANVIL_ATTEMPTS} attempts (exited before its RPC came up each time)"
                );
            }
        };
        let manifest = contracts.join(format!("deployments/{chain_id}.json"));

        // Deploy mock USDC, then the protocol with the initial TOKEN supply held
        // by the admin EOA so it can distribute bond stake to N operators.
        let usdc = deploy_mock_usdc(&admin, &contracts).await?;
        let admin_token_holder: Address = ADMIN_ADDR.parse().context("parse admin addr")?;
        run_deploy_script(&contracts, &rpc_url, usdc, admin_token_holder).await?;
        let addrs = read_manifest(&manifest)?;

        Ok(Self {
            _anvil: anvil,
            url,
            chain_id,
            addrs,
            usdc,
            admin,
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

    /// Add `hash` to `region`'s blacklist as governance. The Timelock holds
    /// `DEFAULT_ADMIN_ROLE`, so it grants itself `REGIONAL_BODY_ROLE` first (a
    /// no-op on repeat), then adds the entry. `region` is the packed key from
    /// [`region_key`]. Emits `HashBlacklisted(region, hash)`.
    pub async fn add_hash_regional(&self, region: B256, hash: B256) -> anyhow::Result<()> {
        self.impersonate(self.addrs.timelock).await?;
        let raw = self.raw_provider();
        let grant = AccessControl::new(self.addrs.content_blacklist, &raw)
            .grantRole(regional_body_role(), self.addrs.timelock)
            .from(self.addrs.timelock)
            .send()
            .await
            .context("grantRole REGIONAL_BODY_ROLE send")?
            .get_receipt()
            .await
            .context("grantRole receipt")?;
        crate::ensure_mined(&grant, "grantRole")?;
        let receipt = ContentBlacklist::new(self.addrs.content_blacklist, &raw)
            .addHashRegional(region, hash, "e2e-takedown".to_string())
            .from(self.addrs.timelock)
            .send()
            .await
            .context("addHashRegional send")?
            .get_receipt()
            .await
            .context("addHashRegional receipt")?;
        crate::ensure_mined(&receipt, "addHashRegional")
    }

    /// Change an operator's self-attested region via `CapacityBond.updateRegion`
    /// (ADR 030). Sent by the operator itself. The first change has no cooldown
    /// (`regionLastChanged` is 0 until the first update). Used to exercise a
    /// scope transition that emits no `ContentBlacklist` event.
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

    /// Slash `operator` through a real `SlashJudge` phantom-announcement
    /// commit-reveal challenge (#1032, G-NODE-05). Builds the operator's own
    /// self-incriminating probe (`hasBlob=true`) + stream (`ok=false`) evidence
    /// for the same `blob_hash`, signs it with the operator's eth key over the
    /// `SlashJudge` EIP-712 domain, commits, warps past `MIN_REVEAL_DELAY`, and
    /// reveals as `challenger`. Returns the minted `slashId`.
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
        // Arm the challenger: gas + the refundable challenge bond, approved to
        // the judge (any funded EOA may challenge).
        self.fund_eth(challenger.address(), 10).await?;
        let judge_read = SlashJudge::new(self.addrs.slash_judge, &self.admin);
        let bond = judge_read
            .challengeBond()
            .call()
            .await
            .context("read challengeBond")?;
        self.transfer_token(challenger.address(), bond).await?;
        let ch_provider = self.provider_for(challenger);
        let approve = Erc20::new(self.addrs.token, &ch_provider)
            .approve(self.addrs.slash_judge, bond)
            .send()
            .await
            .context("challenger token.approve send")?
            .get_receipt()
            .await
            .context("challenger token.approve receipt")?;
        crate::ensure_mined(&approve, "challenger token.approve")?;

        // Evidence timestamps anchored just behind the current chain time
        // (within the 5-day age bound and the 30s probe↔stream window).
        let now = self.head_timestamp().await?;
        let probe_ts_us = (now - 10) * 1_000_000;
        let stream_ts_us = (now - 5) * 1_000_000;
        let rate: u64 = 10;
        let total_bytes: u64 = 1_048_576;
        let channel_id = B256::from(U256::from(1u64));

        let probe = ProbeSlashData {
            hash: blob_hash,
            has_blob: true,
            rate_per_mb: rate,
            timestamp_us: probe_ts_us,
        };
        let stream = StreamSlashData {
            hash: blob_hash,
            ok: false,
            rate_per_mb: rate,
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

        // evidenceHash = keccak256(abi.encode(uint8(Phantom), probeStructHash,
        // streamStructHash)); commitment binds it to (salt, challenger).
        // `abi.encode(uint8 v)` right-aligns `v` in a 32-byte word — byte-
        // identical to `uint256(v)`, which alloy's `SolValue` encodes directly.
        let offense = U256::from(0u8); // OffenseType.Phantom
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
            ratePerMb: rate,
            timestampUs: probe_ts_us,
        };
        let stream_msg = SlashJudge::StreamMsg {
            hash: blob_hash,
            ok: false,
            ratePerMb: rate,
            totalBytes: total_bytes,
            channelId: channel_id,
            timestampUs: stream_ts_us,
            redirect: B256::ZERO,
        };
        let receipt = judge
            .submitPhantomChallenge(
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
            .context("submitPhantomChallenge send")?
            .get_receipt()
            .await
            .context("submitPhantomChallenge receipt")?;
        crate::ensure_mined(&receipt, "submitPhantomChallenge")?;

        for log in receipt.inner.logs() {
            if let Ok(ev) = SlashJudge::Slashed::decode_log_data(&log.inner.data) {
                return Ok(ev.slashId);
            }
        }
        anyhow::bail!("submitPhantomChallenge mined but emitted no Slashed event")
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
        match forge_output(build_cmd, FORGE_BUILD_TIMEOUT, "forge build").await? {
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
            // ADR 019 § Terms Acceptance — DeployProtocol.s.sol requires a
            // non-zero genesis terms hash (CapacityBond rejects the zero
            // sentinel); registration reads it back from the contract.
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

/// `keccak256("REGIONAL_BODY_ROLE")` — the role `addHashRegional` requires.
fn regional_body_role() -> B256 {
    keccak256(b"REGIONAL_BODY_ROLE")
}

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
        slash_appeal: get("SlashAppeal")?,
        governor: get("DecdnGovernor")?,
        timelock: get("TimelockController")?,
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
