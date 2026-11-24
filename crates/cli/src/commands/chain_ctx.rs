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
/// `[identity]`, `network.bind_port`, `network.external_addrs`, and `[cache]`
/// pull-through flag and origin-table presence these commands need.
/// The file is read raw: no `${VAR}` expansion.
/// Deliberately partial (like `node.rs`'s `AdminPortConfig`) so an operator's
/// typo in an unrelated section can't block onboarding; serde-toml ignores
/// unknown fields. A field read here is typed, so a wrongly typed value in it
/// (a quoted number, a `"yes"` flag) fails the parse, as it fails the
/// daemon's.
#[derive(Debug, Default, Deserialize)]
pub struct FileConfig {
    blockchain: Option<FileBlockchain>,
    identity: Option<FileIdentity>,
    network: Option<FileNetwork>,
    cache: Option<FileCache>,
}

impl FileConfig {
    /// The daemon's QUIC bind port, resolved as the daemon resolves it (see
    /// [`decdn_common::config::configured_bind_port`]).
    pub fn bind_port(&self) -> u16 {
        decdn_common::config::configured_bind_port(self.network.as_ref().and_then(|n| n.bind_port))
    }

    /// `network.external_addrs`, validated by the daemon's rule (see
    /// [`decdn_common::config::configured_external_addrs`]): public `IP:port`
    /// addresses in configured order, each kept once. Empty when unset.
    ///
    /// The CLI reads the file raw, so an entry that holds `${VAR}` fails here
    /// although the daemon expands it. The operator passes `--multiaddr`
    /// instead.
    ///
    /// # Errors
    ///
    /// Returns the first bad entry's error, under `network.external_addrs[i]`.
    pub fn external_addrs(&self) -> anyhow::Result<Vec<std::net::SocketAddr>> {
        decdn_common::config::configured_external_addrs(
            self.network
                .as_ref()
                .and_then(|n| n.external_addrs.as_deref())
                .unwrap_or_default(),
        )
    }

    /// `blockchain.rpc_url`, when the file sets it.
    pub fn rpc_url(&self) -> Option<&str> {
        self.blockchain.as_ref().and_then(|b| b.rpc_url.as_deref())
    }

    /// `blockchain.chain_id`, when the file sets it.
    pub fn chain_id(&self) -> Option<u64> {
        self.blockchain.as_ref().and_then(|b| b.chain_id)
    }

    /// The `[[blockchain.payment_chains]]` entries, in configured order.
    pub fn payment_chains(&self) -> &[FilePaymentChain] {
        self.blockchain
            .as_ref()
            .map_or(&[], |b| b.payment_chains.as_slice())
    }

    /// `blockchain.cctp_sweep_receiver_address`, when the file sets it.
    pub fn cctp_sweep_receiver_address(&self) -> Option<&str> {
        self.blockchain
            .as_ref()
            .and_then(|b| b.cctp_sweep_receiver_address.as_deref())
    }

    /// `blockchain.cctp_iris_url`, when the file sets it.
    pub fn cctp_iris_url(&self) -> Option<&str> {
        self.blockchain
            .as_ref()
            .and_then(|b| b.cctp_iris_url.as_deref())
    }

    /// `blockchain.eth_keystore`, when the file sets it.
    pub fn eth_keystore(&self) -> Option<&Path> {
        self.blockchain
            .as_ref()
            .and_then(|b| b.eth_keystore.as_deref())
    }

    /// `identity.data_dir`, when the file sets it.
    pub fn data_dir(&self) -> Option<&Path> {
        self.identity.as_ref().and_then(|i| i.data_dir.as_deref())
    }

    /// `blockchain.buyer_working_deposit_micro_usdc`, or its default: what the
    /// node escrows when it opens the pool its cache-miss pulls pay from.
    pub fn buyer_working_deposit_micro_usdc(&self) -> u64 {
        self.blockchain
            .as_ref()
            .and_then(|b| b.buyer_working_deposit_micro_usdc)
            .unwrap_or(decdn_common::config::DEFAULT_BUYER_WORKING_DEPOSIT_MICRO_USDC)
    }

    /// Whether `cache.node_to_node_pull_through_enabled` resolves on, by the
    /// daemon's rule ([`decdn_common::config::role_derived_pull_through`]):
    /// the explicit value, else on exactly when no origin backend is set.
    pub fn pull_through(&self) -> bool {
        let cache = self.cache.as_ref();
        decdn_common::config::role_derived_pull_through(
            cache.and_then(|c| c.node_to_node_pull_through_enabled),
            cache.is_some_and(|c| c.origin.is_some() || c.origins.is_some()),
        )
    }

    /// `blockchain.get_logs_max_block_span`, when the file sets it.
    pub fn get_logs_max_block_span(&self) -> Option<u64> {
        self.blockchain
            .as_ref()
            .and_then(|b| b.get_logs_max_block_span)
    }
}

#[derive(Debug, Default, Deserialize)]
struct FileNetwork {
    bind_port: Option<u16>,
    external_addrs: Option<Vec<String>>,
}

/// The `[cache]` fields that decide pull-through. The origin tables are read
/// for presence only, so a malformed origin cannot fail the parse. Presence
/// and the daemon's resolved origins disagree only on configs the daemon
/// refuses to start with (`origins = []`, both tables set, a malformed
/// origin), so presence is enough here.
#[derive(Debug, Default, Deserialize)]
struct FileCache {
    node_to_node_pull_through_enabled: Option<bool>,
    origin: Option<serde::de::IgnoredAny>,
    origins: Option<serde::de::IgnoredAny>,
}

#[derive(Debug, Default, Deserialize)]
struct FileBlockchain {
    rpc_url: Option<String>,
    chain_id: Option<u64>,
    capacity_bond_address: Option<String>,
    eth_keystore: Option<PathBuf>,
    publisher_registry_address: Option<String>,
    origin_assignment_address: Option<String>,
    #[serde(default)]
    payment_chains: Vec<FilePaymentChain>,
    cctp_sweep_receiver_address: Option<String>,
    cctp_iris_url: Option<String>,
    get_logs_max_block_span: Option<u64>,
    buyer_working_deposit_micro_usdc: Option<u64>,
}

/// The fields of a `[[blockchain.payment_chains]]` entry these commands read.
#[derive(Debug, Default, Deserialize)]
pub struct FilePaymentChain {
    /// The payment chain's JSON-RPC endpoint.
    pub rpc_url: Option<String>,
    /// The payment chain's EIP-155 id.
    pub chain_id: Option<u64>,
    /// The `PaymentPool` addresses on it, as written.
    #[serde(default)]
    pub payment_pools: Vec<String>,
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
/// Kept separate so [`resolve`] and [`resolve_publish`] can't drift on this
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
    // Shared with `resolve` so the rpc_url/chain_id/
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
