//! Shared plumbing for the on-chain `node` subcommands (`register`, `bond`).
//!
//! Resolves the blockchain coordinates (flag > `[blockchain]`/`[identity]`
//! TOML config > default) and loads the operator's keystore signer. Commands
//! then build a wallet-filled provider with
//! [`decdn_client::provider::build_provider`] and construct their
//! `CapacityBond` instance against it.

use std::io;
use std::path::{Path, PathBuf};

use alloy::primitives::Address;
use alloy::providers::Provider;
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use decdn_common::cli;
use decdn_common::cli::common::expand_tilde;
use decdn_common::config::DEFAULT_CHAIN_ID;
use decdn_incentive::eth_identity::{self, PasswordSource, PasswordUse, ResolvedPassword};
use serde::Deserialize;

/// Partial deserializer for the TOML config — only the `[blockchain]`,
/// `[identity]`, and `network.bind_port` fields these commands need.
/// Deliberately partial (like `node.rs`'s `AdminPortConfig`) so an operator's
/// typo in an unrelated section can't block onboarding; serde-toml ignores
/// unknown fields.
#[derive(Debug, Default, Deserialize)]
pub struct FileConfig {
    blockchain: Option<FileBlockchain>,
    identity: Option<FileIdentity>,
    network: Option<FileNetwork>,
}

impl FileConfig {
    /// The daemon's QUIC bind port, resolved as the daemon resolves it (see
    /// [`decdn_common::config::configured_bind_port`]).
    pub fn bind_port(&self) -> u16 {
        decdn_common::config::configured_bind_port(self.network.as_ref().and_then(|n| n.bind_port))
    }
}

#[derive(Debug, Default, Deserialize)]
struct FileNetwork {
    bind_port: Option<u16>,
}

#[derive(Debug, Default, Deserialize)]
struct FileBlockchain {
    rpc_url: Option<String>,
    chain_id: Option<u64>,
    capacity_bond_address: Option<String>,
    eth_keystore: Option<PathBuf>,
    publisher_registry_address: Option<String>,
    origin_assignment_address: Option<String>,
    slash_appeal_address: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct FileIdentity {
    data_dir: Option<PathBuf>,
}

/// Coordinates resolved from flags > config file > defaults.
#[derive(Debug)]
pub struct Resolved {
    /// JSON-RPC endpoint the command talks to.
    pub rpc_url: String,
    /// EVM chain id, checked against what the endpoint reports.
    pub chain_id: u64,
    /// `CapacityBond` address the bond and unbond commands act on.
    pub capacity_bond_address: Address,
    /// Path to the operator's Ethereum keystore file.
    pub keystore: PathBuf,
    /// Node data directory; the keystore and node key default to paths
    /// under it.
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

/// Resolve the `rpc_url` / `chain_id` / `data_dir` / `keystore` fields shared by
/// every on-chain command with the same flag > config > default precedence.
/// Kept separate so [`resolve`] and [`resolve_appeal`] can't drift on this
/// chain. `expand_tilde` is applied to whichever explicit path wins (flag OR
/// config); the `default_data_dir` fallback is already absolute.
fn resolve_common(
    chain: &cli::CommonChainArgs,
    file: &FileConfig,
) -> anyhow::Result<(String, u64, PathBuf, PathBuf)> {
    let bc = file.blockchain.as_ref();
    let rpc_url = chain
        .rpc_url
        .clone()
        .or_else(|| bc.and_then(|b| b.rpc_url.clone()))
        .ok_or_else(|| {
            anyhow::anyhow!("rpc_url not set (pass --rpc-url or set blockchain.rpc_url)")
        })?;
    let chain_id = chain
        .chain_id
        .or_else(|| bc.and_then(|b| b.chain_id))
        .unwrap_or(DEFAULT_CHAIN_ID);
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
    Ok((rpc_url, chain_id, data_dir, keystore))
}

/// Resolve the chain coordinates with flag > config > default precedence.
/// Pure so the precedence is unit-testable.
pub fn resolve(chain: &cli::ChainArgs, file: &FileConfig) -> anyhow::Result<Resolved> {
    let bc = file.blockchain.as_ref();
    // `resolve_common` first so `rpc_url` is the first missing-field reported
    // (preserves the original error priority before the shared extraction).
    let (rpc_url, chain_id, data_dir, keystore) = resolve_common(&chain.common, file)?;
    let capacity_bond_raw = chain
        .capacity_bond_address
        .clone()
        .or_else(|| bc.and_then(|b| b.capacity_bond_address.clone()))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "capacity_bond_address not set (pass --capacity-bond-address or set \
                 blockchain.capacity_bond_address)"
            )
        })?;
    let capacity_bond_address = parse_nonzero_address(&capacity_bond_raw, "capacity_bond_address")?;
    Ok(Resolved {
        rpc_url,
        chain_id,
        capacity_bond_address,
        keystore,
        data_dir,
    })
}

/// Coordinates for `decdn appeal slash`, resolved from flags > config >
/// defaults. Requires `rpc_url` + `slash_appeal_address`; unlike [`Resolved`]
/// it does *not* require `capacity_bond_address` (the appeal path doesn't touch
/// `CapacityBond` directly).
#[derive(Debug)]
pub struct ResolvedAppeal {
    /// JSON-RPC endpoint the command talks to.
    pub rpc_url: String,
    /// EVM chain id, checked against what the endpoint reports.
    pub chain_id: u64,
    /// `SlashAppeal` address the appeal is filed against.
    pub slash_appeal_address: Address,
    /// Path to the operator's Ethereum keystore file.
    pub keystore: PathBuf,
    /// Node data directory; the keystore and node key default to paths
    /// under it.
    pub data_dir: PathBuf,
}

/// Resolve appeal-command coordinates. Pure so precedence is unit-testable.
/// `slash_appeal_flag` is the command's `--slash-appeal-address` override.
pub fn resolve_appeal(
    chain: &cli::CommonChainArgs,
    slash_appeal_flag: Option<&str>,
    file: &FileConfig,
) -> anyhow::Result<ResolvedAppeal> {
    let bc = file.blockchain.as_ref();
    let slash_appeal_raw = slash_appeal_flag
        .map(str::to_string)
        .or_else(|| bc.and_then(|b| b.slash_appeal_address.clone()))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "slash_appeal_address not set (pass --slash-appeal-address or set \
                 blockchain.slash_appeal_address)"
            )
        })?;
    // Reject the zero address via the shared guard (#1153) — see
    // `parse_nonzero_address`; each of the four addresses resolved here and in
    // `resolve` / `resolve_publish` gets the same early, labelled rejection.
    let slash_appeal_address = parse_nonzero_address(&slash_appeal_raw, "slash_appeal_address")?;
    let (rpc_url, chain_id, data_dir, keystore) = resolve_common(chain, file)?;
    Ok(ResolvedAppeal {
        rpc_url,
        chain_id,
        slash_appeal_address,
        keystore,
        data_dir,
    })
}

// Address parsing lives in `decdn-common` so the zero-address guard is shared,
// not duplicated, across the daemon, this CLI, and `decdn-incentive` (#1219).
// Re-exported here so the `chain_ctx::parse_address` / `chain_ctx::parse_nonzero_address`
// call sites across the CLI keep resolving unchanged.
pub use decdn_common::address::{parse_address, parse_nonzero_address};

/// Load the operator's Ethereum keystore signer, sourcing the password per
/// `password_sources` — the same precedence the daemon uses.
///
/// `load_signer` runs the keystore's scrypt KDF, which is CPU-heavy
/// (hundreds of ms); it is offloaded to `spawn_blocking` so it doesn't stall
/// the async executor, matching `decdn-node`'s runtime keystore load.
pub async fn load_operator_signer(
    chain: &cli::CommonChainArgs,
    keystore: &Path,
) -> anyhow::Result<PrivateKeySigner> {
    load_signer_with_password_file(chain.keystore_password_file.as_deref(), keystore).await
}

/// CLI-boundary wrapper over [`decdn_incentive::eth_identity::standard_sources`],
/// the shared keystore password source list. Precedence, presence semantics,
/// and `usage` are documented there; the `decdn-node` daemon reaches the same
/// builder directly.
///
/// The delta this wrapper adds is tilde expansion, so callers passing the raw
/// clap value (`key-gen`, the operator commands) need not do it themselves.
/// `fetch` and `pool` expand at their own `resolve_chain`, and `expand_tilde`
/// leaves an already-expanded path alone, so the second pass costs them
/// nothing. Expansion rewrites a leading `~` and nothing else: a relative path
/// stays relative and resolves against the working directory when read.
pub(crate) fn password_sources(
    password_file: Option<&Path>,
    usage: PasswordUse,
) -> Vec<PasswordSource> {
    eth_identity::standard_sources(password_file.map(expand_tilde), usage)
}

/// Resolve the keystore password and print any source warnings to stderr. The
/// shared CLI entry point to [`eth_identity::read_password`]: `decdn-incentive`
/// denies `print_stderr`, so it returns the warnings for a caller to surface,
/// and the CLI is where a mistyped `--keystore-password-file` is typed.
///
/// Returns the full [`ResolvedPassword`] so a caller that must inspect the
/// winning source — `key-gen`'s empty-password guard — still can; callers that
/// only need the secret take [`ResolvedPassword::into_secret`].
pub(crate) fn read_keystore_password(
    sources: &[PasswordSource],
    prompt_label: &str,
) -> anyhow::Result<ResolvedPassword> {
    let resolved = eth_identity::read_password(sources, prompt_label)?;
    for warning in resolved.warnings() {
        eprintln!("warning: {warning}");
    }
    Ok(resolved)
}

/// Load an Ethereum keystore signer, sourcing the password per
/// `password_sources`. The scrypt KDF is offloaded to `spawn_blocking` so it
/// doesn't stall the async executor.
///
/// `password_file` is the raw `--keystore-password-file` value; expansion is
/// `password_sources`' job. Most callers arrive through
/// [`load_operator_signer`], which pulls that path off the shared
/// [`cli::CommonChainArgs`].
pub async fn load_signer_with_password_file(
    password_file: Option<&Path>,
    keystore: &Path,
) -> anyhow::Result<PrivateKeySigner> {
    let password = read_keystore_password(
        &password_sources(password_file, PasswordUse::Unlock),
        "eth keystore password",
    )?
    .into_secret();
    let keystore = keystore.to_path_buf();
    let display = keystore.display().to_string();
    tokio::task::spawn_blocking(move || eth_identity::load_signer(&keystore, &password))
        .await
        .context("keystore decryption task panicked")?
        .with_context(|| format!("failed to load keystore at {display}"))
}

/// Publisher-command coordinates resolved from flags > config > defaults.
/// The two contract addresses are optional here; each subcommand requires
/// only the one it targets and errors with a specific message if it is unset.
#[derive(Debug)]
pub struct ResolvedPublish {
    /// JSON-RPC endpoint the command talks to.
    pub rpc_url: String,
    /// EVM chain id, checked against what the endpoint reports.
    pub chain_id: u64,
    /// `PublisherRegistry` address. `None` when neither
    /// `--publisher-registry-address` nor `blockchain.publisher_registry_address`
    /// is set; a subcommand that needs it then errors naming the missing key.
    pub publisher_registry_address: Option<Address>,
    /// `OriginAssignment` address, optional on the same terms.
    pub origin_assignment_address: Option<Address>,
    /// Path to the operator's Ethereum keystore file.
    pub keystore: PathBuf,
    /// Node data directory; the keystore and node key default to paths
    /// under it.
    pub data_dir: PathBuf,
}

/// Resolve publisher-command coordinates. Pure so precedence is unit-testable.
pub fn resolve_publish(
    args: &cli::PublishChainArgs,
    file: &FileConfig,
) -> anyhow::Result<ResolvedPublish> {
    let bc = file.blockchain.as_ref();
    // Shared with `resolve` / `resolve_appeal` so the rpc_url/chain_id/
    // data_dir/keystore precedence can't drift between the `node` and
    // `publish` commands. Only the two contract addresses are publish-specific.
    let (rpc_url, chain_id, data_dir, keystore) = resolve_common(&args.common, file)?;
    let publisher_registry_address = args
        .publisher_registry_address
        .clone()
        .or_else(|| bc.and_then(|b| b.publisher_registry_address.clone()))
        .map(|raw| parse_nonzero_address(&raw, "publisher_registry_address"))
        .transpose()?;
    let origin_assignment_address = args
        .origin_assignment_address
        .clone()
        .or_else(|| bc.and_then(|b| b.origin_assignment_address.clone()))
        .map(|raw| parse_nonzero_address(&raw, "origin_assignment_address"))
        .transpose()?;
    Ok(ResolvedPublish {
        rpc_url,
        chain_id,
        publisher_registry_address,
        origin_assignment_address,
        keystore,
        data_dir,
    })
}

/// The shared transaction submitter, re-exported so the on-chain `node`
/// subcommands keep their `chain_ctx::send(..)` call shape. It lives in
/// `decdn-incentive` so every on-chain write in that crate shares it (#1355). See
/// [`decdn_incentive::tx::send`] for the `landed` contract, which callers must
/// read carefully: an `Err` does **not** imply nothing landed.
pub(crate) use decdn_incentive::tx::send;

/// Head block timestamp: the clock a contract compares `block.timestamp`
/// against. A pre-check that reads it, rather than the local clock, agrees
/// with the contract even when this machine's clock drifts.
///
/// # Errors
///
/// Errors when the latest block cannot be read.
pub(crate) async fn head_timestamp<P: Provider>(provider: &P) -> anyhow::Result<u64> {
    let block = provider
        .get_block(alloy::eips::BlockId::latest())
        .await
        .context("failed to read the latest block")?
        .ok_or_else(|| anyhow::anyhow!("no latest block"))?;
    Ok(block.header.timestamp)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
