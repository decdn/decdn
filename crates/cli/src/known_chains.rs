//! Registry of deployment targets `decdn config init --chain <name>` can bake in.
//!
//! A deployment target is one **governance chain** (the protocol contracts:
//! `CapacityBond`, `SlashJudge`, `ContentBlacklist`, `CctpSweepReceiver`, …)
//! and one or more **payment chains**, each running a `PaymentPool`. Each
//! [`KnownChain`] pairs a small hand-written descriptor (name, label, chain
//! ids, public RPC endpoints) with the deployment manifests embedded at compile
//! time: the governance manifest `<chainId>.json` the Foundry `DeployProtocol`
//! script writes, and one `<chainId>-payment.json` per payment chain from
//! `DeployPaymentPool`. Contract addresses are never written here — they are
//! read from the manifests, so a redeploy flows into `config init`
//! automatically with no addresses to hand-sync.
//!
//! The manifests are embedded from `crates/cli/deployments/`, not from
//! `contracts/deployments/` where the deploy scripts write them, because
//! `include_str!` may not escape the package: a path outside the crate is
//! unreachable from the published `.crate` and `cargo install decdn-cli` would
//! fail to build. `contracts/deployments/` stays canonical and this is a
//! byte-identical mirror — the `deployment-manifest-mirror` pre-commit hook and
//! its CI counterpart `cmp` the two and fail on any drift, so re-copy the files
//! as part of a redeploy.

use std::collections::BTreeMap;

use anyhow::Context;
use serde::Deserialize;

/// A deployment target `config init` knows how to bake into a fresh config.
#[derive(Debug)]
pub struct KnownChain {
    /// `--chain` selector, e.g. `"arbitrum-sepolia"`.
    pub name: &'static str,
    /// Human-readable label for generated comments and error messages.
    pub label: &'static str,
    /// The governance chain's EIP-155 id; must match the governance
    /// manifest's `chainId`.
    pub chain_id: u64,
    /// The governance chain's public JSON-RPC endpoint baked into the emitted
    /// config. Fine for light use; operators point this at their own provider
    /// for production.
    pub public_rpc: &'static str,
    /// The L2 block the governance contracts were created in: where a
    /// from-genesis log scan starts. A manifest's `deployBlock` is not this
    /// number: the deploy script writes `block.number`, which Arbitrum reports
    /// as the L1 block, and `eth_getLogs` takes L2 numbers. Update it with the
    /// manifest on a redeploy, from the earliest contract creation block on the
    /// explorer.
    pub l2_start_block: u64,
    /// Where an operator gets this target's payment token, for a target that
    /// has a public faucet. `decdn setup` points an unfunded buyer wallet here.
    pub usdc_faucet: Option<&'static str>,
    /// Base URL of Circle's CCTP attestation service (Iris) for this target:
    /// the sandbox for a testnet, production for mainnet. `decdn node
    /// receive-sweeps` reads it when the config sets no
    /// `blockchain.cctp_iris_url`.
    pub cctp_iris_url: &'static str,
    /// The embedded governance manifest `deployments/<chainId>.json`.
    pub(crate) governance_manifest: &'static str,
    /// The payment chains, in the order `config init` writes them.
    pub(crate) payment_chains: &'static [KnownPaymentChain],
}

/// One payment chain of a [`KnownChain`].
#[derive(Debug)]
pub struct KnownPaymentChain {
    /// Human-readable label for generated comments.
    pub label: &'static str,
    /// The payment chain's EIP-155 id; must match its manifest's `chainId`.
    pub chain_id: u64,
    /// The payment chain's public JSON-RPC endpoint.
    pub public_rpc: &'static str,
    /// The block the `PaymentPool` was created in: where a from-genesis log
    /// scan of the pool starts (`decdn node earnings --address`, `decdn node
    /// receive-sweeps`). Update it with the manifest on a redeploy.
    pub start_block: u64,
    /// The embedded payment manifest `deployments/<chainId>-payment.json`.
    pub(crate) manifest: &'static str,
}

/// Every target `config init` can target. Adding an entry here — plus its
/// manifests under `contracts/deployments/` and a copy of each in
/// `crates/cli/deployments/` — is all it takes to offer a new network.
pub const KNOWN_CHAINS: &[KnownChain] = &[];

/// The known target whose governance chain has EIP-155 id `chain_id`, if any.
#[must_use]
pub fn by_chain_id(chain_id: u64) -> Option<&'static KnownChain> {
    KNOWN_CHAINS.iter().find(|chain| chain.chain_id == chain_id)
}

/// Governance contract addresses read from a target's governance manifest,
/// mapped to `[blockchain]` config keys. Every field is an EIP-55 checksummed
/// `0x` string exactly as the deploy script recorded it.
#[derive(Debug)]
pub struct ChainAddresses {
    /// `blockchain.capacity_bond_address`.
    pub capacity_bond: String,
    /// `blockchain.slash_judge_address`.
    pub slash_judge: String,
    /// `blockchain.content_blacklist_address`.
    pub content_blacklist: String,
    /// `blockchain.origin_assignment_address`.
    pub origin_assignment: String,
    /// `blockchain.publisher_registry_address`.
    pub publisher_registry: String,
    /// `blockchain.slash_appeal_address`.
    pub slash_appeal: String,
    /// `blockchain.cctp_sweep_receiver_address`.
    pub cctp_sweep_receiver: String,
}

/// One payment chain's coordinates, read from its payment manifest: a
/// `[[blockchain.payment_chains]]` entry.
#[derive(Debug, PartialEq, Eq)]
pub struct PaymentChainAddresses {
    /// Human-readable label for generated comments.
    pub label: &'static str,
    /// `chain_id`.
    pub chain_id: u64,
    /// `rpc_url`: the payment chain's public endpoint.
    pub public_rpc: &'static str,
    /// The block the pool was created in ([`KnownPaymentChain::start_block`]).
    pub start_block: u64,
    /// `payment_pools`: the manifest's `PaymentPool`.
    pub payment_pools: Vec<String>,
    /// The payment token on this chain, the manifest's `externalDeps.usdc`.
    /// A pool reads the same address as `usdc()`.
    pub usdc: String,
}

/// The subset of a deployment manifest `config init` consumes. Unknown
/// manifest fields (deploy block, parameters, …) are ignored.
#[derive(Deserialize)]
struct DeploymentManifest {
    #[serde(rename = "chainId")]
    chain_id: u64,
    contracts: BTreeMap<String, String>,
    #[serde(rename = "externalDeps", default)]
    external_deps: BTreeMap<String, String>,
}

impl DeploymentManifest {
    /// Parse `json` and check its `chainId` is `chain_id`. `what` names the
    /// manifest in errors.
    fn parse(json: &str, chain_id: u64, what: &str) -> anyhow::Result<Self> {
        let manifest: Self = serde_json::from_str(json)
            .with_context(|| format!("parsing the embedded {what} deployment manifest"))?;
        anyhow::ensure!(
            manifest.chain_id == chain_id,
            "{what} deployment manifest chainId {} does not match registry chain_id {chain_id}",
            manifest.chain_id,
        );
        Ok(manifest)
    }

    /// The contract `key`, or an error naming the manifest.
    fn contract(&self, key: &str, what: &str) -> anyhow::Result<String> {
        self.contracts.get(key).cloned().ok_or_else(|| {
            anyhow::anyhow!("{what} deployment manifest is missing contract `{key}`")
        })
    }
}

/// Read the governance addresses out of a governance manifest for chain
/// `chain_id`. `what` names the manifest in errors.
///
/// # Errors
///
/// The manifest does not parse, names another chain, or lacks a contract.
pub(crate) fn parse_governance_manifest(
    json: &str,
    chain_id: u64,
    what: &str,
) -> anyhow::Result<ChainAddresses> {
    let manifest = DeploymentManifest::parse(json, chain_id, what)?;
    Ok(ChainAddresses {
        capacity_bond: manifest.contract("CapacityBond", what)?,
        slash_judge: manifest.contract("SlashJudge", what)?,
        content_blacklist: manifest.contract("ContentBlacklist", what)?,
        origin_assignment: manifest.contract("OriginAssignment", what)?,
        publisher_registry: manifest.contract("PublisherRegistry", what)?,
        slash_appeal: manifest.contract("SlashAppeal", what)?,
        cctp_sweep_receiver: manifest.contract("CctpSweepReceiver", what)?,
    })
}

/// Read one payment chain's pool and token out of its payment manifest.
///
/// # Errors
///
/// The manifest does not parse, names another chain, or lacks the
/// `PaymentPool` or the `usdc` dependency.
pub(crate) fn parse_payment_manifest(
    chain: &KnownPaymentChain,
) -> anyhow::Result<PaymentChainAddresses> {
    let what = format!("{} payment", chain.label);
    let manifest = DeploymentManifest::parse(chain.manifest, chain.chain_id, &what)?;
    let usdc = manifest.external_deps.get("usdc").cloned().ok_or_else(|| {
        anyhow::anyhow!("{what} deployment manifest is missing externalDeps.usdc")
    })?;
    Ok(PaymentChainAddresses {
        label: chain.label,
        chain_id: chain.chain_id,
        public_rpc: chain.public_rpc,
        start_block: chain.start_block,
        payment_pools: vec![manifest.contract("PaymentPool", &what)?],
        usdc,
    })
}

impl KnownChain {
    /// Parse the embedded governance manifest and pull the addresses `config
    /// init` bakes in. Fails (rather than panics) if the manifest is
    /// malformed, its `chainId` disagrees with this entry, or an expected
    /// contract is absent.
    ///
    /// # Errors
    ///
    /// The manifest does not parse, names another chain, or lacks a contract.
    pub fn addresses(&self) -> anyhow::Result<ChainAddresses> {
        parse_governance_manifest(self.governance_manifest, self.chain_id, self.name)
    }

    /// Parse every embedded payment manifest, in order.
    ///
    /// # Errors
    ///
    /// A manifest does not parse, names another chain, or lacks the
    /// `PaymentPool` or the `usdc` dependency.
    pub fn payment_chains(&self) -> anyhow::Result<Vec<PaymentChainAddresses>> {
        self.payment_chains
            .iter()
            .map(parse_payment_manifest)
            .collect()
    }
}

/// Comma-separated list of known selectors, for help and error text.
fn known_names() -> String {
    KNOWN_CHAINS
        .iter()
        .map(|c| c.name)
        .collect::<Vec<_>>()
        .join(", ")
}

/// Resolve a `--chain` selector to the target whose values to bake in.
///
/// - `Some("none")` => `Ok(None)`: emit the blank generic template.
/// - `Some(name)` => the matching target, or an error naming the known ones.
/// - `None` (flag omitted) => the sole known target when exactly one exists,
///   the blank template when none does; otherwise an error asking the operator
///   to choose.
///
/// # Errors
///
/// An unknown selector, or an omitted one with several targets to choose from.
pub fn resolve(selector: Option<&str>) -> anyhow::Result<Option<&'static KnownChain>> {
    resolve_in(KNOWN_CHAINS, selector)
}

/// [`resolve`] over `chains`.
fn resolve_in(
    chains: &'static [KnownChain],
    selector: Option<&str>,
) -> anyhow::Result<Option<&'static KnownChain>> {
    match selector {
        Some("none") => Ok(None),
        Some(name) => chains
            .iter()
            .find(|c| c.name == name)
            .map(Some)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "unknown --chain `{name}`; known chains: {} (or `none` for a blank template)",
                    known_names()
                )
            }),
        None => match chains {
            [] => Ok(None),
            [only] => Ok(Some(only)),
            _ => Err(anyhow::anyhow!(
                "several chains are available; pass --chain <name> (one of: {}) or --chain none",
                known_names()
            )),
        },
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
pub(crate) mod tests;
