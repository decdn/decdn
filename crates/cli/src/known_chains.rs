//! Registry of deployment targets `decdn config init --chain <name>` can bake in.
//!
//! Each [`KnownChain`] pairs a small hand-written descriptor (name, label,
//! chain id, public RPC endpoint) with the on-disk deployment manifest embedded
//! at compile time. Contract addresses are never written here — they are read
//! from the manifest the Foundry `DeployProtocol` script writes, so a redeploy
//! flows into `config init` automatically with no addresses to hand-sync.
//!
//! The manifest is embedded from `crates/cli/deployments/`, not from
//! `contracts/deployments/` where the deploy script writes it, because
//! `include_str!` may not escape the package: a path outside the crate is
//! unreachable from the published `.crate` and `cargo install decdn-cli` would
//! fail to build. `contracts/deployments/` stays canonical and this is a
//! byte-identical mirror — the `deployment-manifest-mirror` pre-commit hook and
//! its CI counterpart `cmp` the two and fail on any drift, so re-copy the file
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
    /// EIP-155 chain id; must match the embedded manifest's `chainId`.
    pub chain_id: u64,
    /// Public JSON-RPC endpoint baked into the emitted config. Fine for light
    /// use; operators point this at their own provider for production.
    pub public_rpc: &'static str,
    /// The embedded `deployments/<chainId>.json` manifest — the sole source of
    /// this chain's contract addresses.
    manifest_json: &'static str,
}

/// Every chain `config init` can target. Adding an entry here — plus its
/// manifest under `contracts/deployments/` and a copy of that file in
/// `crates/cli/deployments/` — is all it takes to offer a new network; today
/// there is exactly one, so `--chain` may be omitted entirely.
pub const KNOWN_CHAINS: &[KnownChain] = &[KnownChain {
    name: "arbitrum-sepolia",
    label: "Arbitrum Sepolia testnet",
    chain_id: 421_614,
    public_rpc: "https://sepolia-rollup.arbitrum.io/rpc",
    manifest_json: include_str!("../deployments/421614.json"),
}];

/// Contract addresses read from a chain's manifest, mapped to `[blockchain]`
/// config keys. Every field is an EIP-55 checksummed `0x` string exactly as the
/// deploy script recorded it.
#[derive(Debug)]
pub struct ChainAddresses {
    /// `blockchain.payment_pool_address`.
    pub payment_pool: String,
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
    /// `blockchain.usdc_address` — the payment token on this chain.
    pub usdc: String,
}

/// The subset of `deployments/<chainId>.json` `config init` consumes. Unknown
/// manifest fields (deploy block, governance params, …) are ignored.
#[derive(Deserialize)]
struct DeploymentManifest {
    #[serde(rename = "chainId")]
    chain_id: u64,
    contracts: BTreeMap<String, String>,
    #[serde(rename = "externalDeps")]
    external_deps: ExternalDeps,
}

#[derive(Deserialize)]
struct ExternalDeps {
    usdc: String,
}

impl KnownChain {
    /// Parse the embedded manifest and pull the addresses `config init` bakes
    /// in. Fails (rather than panics) if the manifest is malformed, its
    /// `chainId` disagrees with this entry, or an expected contract is absent.
    pub fn addresses(&self) -> anyhow::Result<ChainAddresses> {
        let manifest: DeploymentManifest =
            serde_json::from_str(self.manifest_json).with_context(|| {
                format!(
                    "parsing the embedded deployment manifest for `{}`",
                    self.name
                )
            })?;

        anyhow::ensure!(
            manifest.chain_id == self.chain_id,
            "deployment manifest chainId {} does not match registry chain_id {} for `{}`",
            manifest.chain_id,
            self.chain_id,
            self.name,
        );

        let contract = |key: &str| -> anyhow::Result<String> {
            manifest.contracts.get(key).cloned().ok_or_else(|| {
                anyhow::anyhow!(
                    "deployment manifest for `{}` is missing contract `{key}`",
                    self.name
                )
            })
        };

        Ok(ChainAddresses {
            payment_pool: contract("PaymentPool")?,
            capacity_bond: contract("CapacityBond")?,
            slash_judge: contract("SlashJudge")?,
            content_blacklist: contract("ContentBlacklist")?,
            origin_assignment: contract("OriginAssignment")?,
            publisher_registry: contract("PublisherRegistry")?,
            slash_appeal: contract("SlashAppeal")?,
            usdc: manifest.external_deps.usdc,
        })
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

/// Resolve a `--chain` selector to the chain whose values to bake in.
///
/// - `Some("none")` => `Ok(None)`: emit the blank generic template.
/// - `Some(name)` => the matching chain, or an error naming the known chains.
/// - `None` (flag omitted) => the sole known chain when exactly one exists;
///   otherwise an error asking the operator to choose.
pub fn resolve(selector: Option<&str>) -> anyhow::Result<Option<&'static KnownChain>> {
    match selector {
        Some("none") => Ok(None),
        Some(name) => KNOWN_CHAINS
            .iter()
            .find(|c| c.name == name)
            .map(Some)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "unknown --chain `{name}`; known chains: {} (or `none` for a blank template)",
                    known_names()
                )
            }),
        None => match KNOWN_CHAINS {
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
mod tests {
    use super::*;

    #[test]
    fn every_known_chain_manifest_parses_and_matches() {
        for chain in KNOWN_CHAINS {
            // The error from `addresses()` names the chain; `parse_contract_address`
            // errors name the field — so `expect` needs no extra interpolation.
            let addrs = chain
                .addresses()
                .expect("known-chain deployment manifest must parse");
            // Every baked field must pass the same EIP-55 check the resolver
            // applies to a user-supplied address — a bad-checksum manifest is
            // caught here at CI, not on an operator's first `config validate`.
            for (field, value) in [
                ("payment_pool", &addrs.payment_pool),
                ("capacity_bond", &addrs.capacity_bond),
                ("slash_judge", &addrs.slash_judge),
                ("content_blacklist", &addrs.content_blacklist),
                ("origin_assignment", &addrs.origin_assignment),
                ("publisher_registry", &addrs.publisher_registry),
                ("slash_appeal", &addrs.slash_appeal),
                ("usdc", &addrs.usdc),
            ] {
                decdn_common::config::parse_contract_address(field, value)
                    .expect("baked contract address must pass the EIP-55 check");
            }
        }
    }

    #[test]
    fn resolve_none_selector_yields_blank_template() {
        assert!(resolve(Some("none")).unwrap().is_none());
    }

    #[test]
    fn resolve_omitted_selector_picks_the_sole_chain() {
        // Precondition for the "omit --chain" UX: exactly one chain today.
        assert_eq!(KNOWN_CHAINS.len(), 1);
        let chain = resolve(None).unwrap().expect("sole chain is selected");
        assert_eq!(chain.name, "arbitrum-sepolia");
    }

    #[test]
    fn resolve_unknown_selector_errors_with_known_names() {
        let err = resolve(Some("mainnet")).unwrap_err().to_string();
        assert!(err.contains("arbitrum-sepolia"), "{err}");
    }
}
