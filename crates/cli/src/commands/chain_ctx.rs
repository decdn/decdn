//! Shared plumbing for the on-chain `node` subcommands (`register`, `bond`).
//!
//! Resolves the blockchain coordinates (flag > `[blockchain]`/`[identity]`
//! TOML config > default) and loads the operator's keystore signer. Commands
//! then build a wallet-filled provider with
//! [`decdn_client_pull::provider::build_provider`] and construct their
//! `CapacityBond` instance against it.

use std::io;
use std::path::{Path, PathBuf};

use alloy::primitives::Address;
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use decdn_common::cli;
use decdn_common::cli::common::expand_tilde;
use decdn_common::config::DEFAULT_CHAIN_ID;
use decdn_incentive::eth_identity::{self, PasswordSource};
use serde::Deserialize;

/// Partial deserializer for the TOML config — only the `[blockchain]` and
/// `[identity]` fields these commands need. Deliberately partial (like
/// `node.rs`'s `AdminPortConfig`) so an operator's typo in an unrelated
/// section can't block onboarding; serde-toml ignores unknown fields.
#[derive(Debug, Default, Deserialize)]
pub struct FileConfig {
    blockchain: Option<FileBlockchain>,
    identity: Option<FileIdentity>,
}

#[derive(Debug, Default, Deserialize)]
struct FileBlockchain {
    rpc_url: Option<String>,
    chain_id: Option<u64>,
    capacity_bond_address: Option<String>,
    eth_keystore: Option<PathBuf>,
}

#[derive(Debug, Default, Deserialize)]
struct FileIdentity {
    data_dir: Option<PathBuf>,
}

/// Coordinates resolved from flags > config file > defaults.
#[derive(Debug)]
pub struct Resolved {
    pub rpc_url: String,
    pub chain_id: u64,
    pub capacity_bond_address: String,
    pub keystore: PathBuf,
    pub data_dir: PathBuf,
}

/// Load the partial config from `config_path` (or the `--config` flag folded
/// in by the caller). An explicit path that is missing/unreadable is an error;
/// the default path simply falls through to an empty config (every field can
/// also come from a flag).
pub fn load_optional_config(config_path: Option<&Path>) -> anyhow::Result<FileConfig> {
    let (path, explicit) = match config_path {
        Some(p) => (expand_tilde(p), true),
        None => match cli::common::default_config_path() {
            Some(p) => (p, false),
            None => return Ok(FileConfig::default()),
        },
    };
    match std::fs::read_to_string(&path) {
        Ok(contents) => toml::from_str(&contents)
            .with_context(|| format!("failed to parse config file {}", path.display())),
        Err(e) if e.kind() == io::ErrorKind::NotFound && !explicit => Ok(FileConfig::default()),
        Err(e) => {
            Err(anyhow::Error::new(e)
                .context(format!("failed to read config file {}", path.display())))
        }
    }
}

/// Resolve the chain coordinates with flag > config > default precedence.
/// Pure so the precedence is unit-testable.
pub fn resolve(chain: &cli::ChainArgs, file: &FileConfig) -> anyhow::Result<Resolved> {
    let bc = file.blockchain.as_ref();
    let rpc_url = chain
        .rpc_url
        .clone()
        .or_else(|| bc.and_then(|b| b.rpc_url.clone()))
        .ok_or_else(|| {
            anyhow::anyhow!("rpc_url not set (pass --rpc-url or set blockchain.rpc_url)")
        })?;
    let capacity_bond_address = chain
        .capacity_bond_address
        .clone()
        .or_else(|| bc.and_then(|b| b.capacity_bond_address.clone()))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "capacity_bond_address not set (pass --capacity-bond-address or set \
                 blockchain.capacity_bond_address)"
            )
        })?;
    let chain_id = chain
        .chain_id
        .or_else(|| bc.and_then(|b| b.chain_id))
        .unwrap_or(DEFAULT_CHAIN_ID);
    // `expand_tilde` is applied to whichever explicit value wins (flag OR
    // config) — config-file paths get `~` expansion too, not just flags. The
    // `default_data_dir` fallback is already absolute.
    let data_dir = chain
        .data_dir
        .clone()
        .or_else(|| file.identity.as_ref().and_then(|i| i.data_dir.clone()))
        .map(|p| expand_tilde(&p))
        .or_else(cli::default_data_dir)
        .ok_or_else(|| {
            anyhow::anyhow!("data_dir not set and no default available (pass --data-dir)")
        })?;
    let keystore = chain
        .keystore
        .clone()
        .or_else(|| bc.and_then(|b| b.eth_keystore.clone()))
        .map_or_else(
            || eth_identity::keystore_path(&data_dir),
            |p| expand_tilde(&p),
        );
    Ok(Resolved {
        rpc_url,
        chain_id,
        capacity_bond_address,
        keystore,
        data_dir,
    })
}

/// Parse a contract/account address with a labelled error.
pub fn parse_address(value: &str, label: &str) -> anyhow::Result<Address> {
    value
        .parse()
        .with_context(|| format!("{label} {value:?} is not a valid address"))
}

/// Load the operator's Ethereum keystore signer, sourcing the password from
/// the `DECDN_KEYSTORE_PASSWORD` env var, then `--keystore-password-file`,
/// then an interactive prompt — the same precedence the daemon uses.
///
/// `load_signer` runs the keystore's scrypt KDF, which is CPU-heavy
/// (hundreds of ms); it is offloaded to `spawn_blocking` so it doesn't stall
/// the async executor, matching `decdn-node`'s runtime keystore load.
pub async fn load_operator_signer(
    chain: &cli::ChainArgs,
    keystore: &Path,
) -> anyhow::Result<PrivateKeySigner> {
    let mut sources = vec![PasswordSource::Env(eth_identity::KEYSTORE_PASSWORD_ENV)];
    if let Some(path) = chain.keystore_password_file.as_deref().map(expand_tilde) {
        sources.push(PasswordSource::File(path));
    }
    sources.push(PasswordSource::Prompt { confirm: false });
    let password = eth_identity::read_password(&sources, "eth keystore password")?;
    let keystore = keystore.to_path_buf();
    let display = keystore.display().to_string();
    tokio::task::spawn_blocking(move || eth_identity::load_signer(&keystore, &password))
        .await
        .context("keystore decryption task panicked")?
        .with_context(|| format!("failed to load keystore at {display}"))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn empty_chain() -> cli::ChainArgs {
        cli::ChainArgs {
            config: None,
            rpc_url: None,
            capacity_bond_address: None,
            chain_id: None,
            keystore: None,
            data_dir: Some(PathBuf::from("/tmp/decdn-test")),
            keystore_password_file: None,
            dry_run: true,
            json: false,
        }
    }

    fn file_with(bc: FileBlockchain) -> FileConfig {
        FileConfig {
            blockchain: Some(bc),
            identity: None,
        }
    }

    #[test]
    fn flags_override_config() {
        let mut chain = empty_chain();
        chain.rpc_url = Some("http://flag:8545".to_string());
        chain.capacity_bond_address = Some("0xFLAG".to_string());
        chain.chain_id = Some(99);
        let file = file_with(FileBlockchain {
            rpc_url: Some("http://config:8545".to_string()),
            chain_id: Some(1),
            capacity_bond_address: Some("0xCONFIG".to_string()),
            eth_keystore: None,
        });
        let r = resolve(&chain, &file).unwrap();
        assert_eq!(r.rpc_url, "http://flag:8545");
        assert_eq!(r.capacity_bond_address, "0xFLAG");
        assert_eq!(r.chain_id, 99);
    }

    #[test]
    fn config_fills_unset_flags() {
        let chain = empty_chain();
        let file = file_with(FileBlockchain {
            rpc_url: Some("http://config:8545".to_string()),
            chain_id: None,
            capacity_bond_address: Some("0xCONFIG".to_string()),
            eth_keystore: Some(PathBuf::from("/keys/ks.json")),
        });
        let r = resolve(&chain, &file).unwrap();
        assert_eq!(r.rpc_url, "http://config:8545");
        assert_eq!(r.capacity_bond_address, "0xCONFIG");
        // chain_id absent everywhere → default.
        assert_eq!(r.chain_id, DEFAULT_CHAIN_ID);
        assert_eq!(r.keystore, PathBuf::from("/keys/ks.json"));
    }

    #[test]
    fn keystore_defaults_under_data_dir() {
        let chain = empty_chain();
        let file = file_with(FileBlockchain {
            rpc_url: Some("http://x".to_string()),
            chain_id: None,
            capacity_bond_address: Some("0xY".to_string()),
            eth_keystore: None,
        });
        let r = resolve(&chain, &file).unwrap();
        assert_eq!(r.keystore, PathBuf::from("/tmp/decdn-test/keystore.json"));
    }

    #[test]
    fn missing_required_fields_error() {
        let chain = empty_chain();
        let err = resolve(&chain, &FileConfig::default()).unwrap_err();
        assert!(err.to_string().contains("rpc_url not set"), "{err}");
    }
}
