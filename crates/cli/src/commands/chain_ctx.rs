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
use decdn_incentive::swap_venue::ResolvedSwap;
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
    publisher_registry_address: Option<String>,
    origin_assignment_address: Option<String>,
    swap_venue: Option<String>,
    swap_router_address: Option<String>,
    swap_quoter_address: Option<String>,
    usdc_address: Option<String>,
    swap_fee_tier: Option<u32>,
    swap_balancer_pool: Option<String>,
    swap_pool_address: Option<String>,
    slash_appeal_address: Option<String>,
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
    pub rpc_url: String,
    pub chain_id: u64,
    pub slash_appeal_address: String,
    pub keystore: PathBuf,
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
    let slash_appeal_address = slash_appeal_flag
        .map(str::to_string)
        .or_else(|| bc.and_then(|b| b.slash_appeal_address.clone()))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "slash_appeal_address not set (pass --slash-appeal-address or set \
                 blockchain.slash_appeal_address)"
            )
        })?;
    // Guard the *consumer*: a zero address parses fine but is never a real
    // deployment (it would surface only as an opaque on-chain revert at appeal
    // time). This is the sole validation site for the appeal address.
    anyhow::ensure!(
        slash_appeal_address
            .trim_start_matches("0x")
            .bytes()
            .any(|b| b != b'0'),
        "slash_appeal_address must not be the zero address — \
         set it to the deployed SlashAppeal contract (ADR 028)"
    );
    let (rpc_url, chain_id, data_dir, keystore) = resolve_common(chain, file)?;
    Ok(ResolvedAppeal {
        rpc_url,
        chain_id,
        slash_appeal_address,
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
    chain: &cli::CommonChainArgs,
    keystore: &Path,
) -> anyhow::Result<PrivateKeySigner> {
    load_signer_with_password_file(chain.keystore_password_file.as_deref(), keystore).await
}

/// Load an Ethereum keystore signer, sourcing the password from the
/// `DECDN_KEYSTORE_PASSWORD` env var, then `password_file`, then an interactive
/// prompt. The scrypt KDF is offloaded to `spawn_blocking` so it doesn't stall
/// the async executor. The `node`, `appeal`, and `publish` commands all reach
/// it through [`load_operator_signer`], which pulls the password-file path off
/// the shared [`cli::CommonChainArgs`].
pub async fn load_signer_with_password_file(
    password_file: Option<&Path>,
    keystore: &Path,
) -> anyhow::Result<PrivateKeySigner> {
    let mut sources = vec![PasswordSource::Env(eth_identity::KEYSTORE_PASSWORD_ENV)];
    if let Some(path) = password_file.map(expand_tilde) {
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

/// Publisher-command coordinates resolved from flags > config > defaults.
/// The two contract addresses are optional here; each subcommand requires
/// only the one it targets and errors with a specific message if it is unset.
#[derive(Debug)]
pub struct ResolvedPublish {
    pub rpc_url: String,
    pub chain_id: u64,
    pub publisher_registry_address: Option<String>,
    pub origin_assignment_address: Option<String>,
    pub keystore: PathBuf,
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
        .or_else(|| bc.and_then(|b| b.publisher_registry_address.clone()));
    let origin_assignment_address = args
        .origin_assignment_address
        .clone()
        .or_else(|| bc.and_then(|b| b.origin_assignment_address.clone()));
    Ok(ResolvedPublish {
        rpc_url,
        chain_id,
        publisher_registry_address,
        origin_assignment_address,
        keystore,
        data_dir,
    })
}

/// Resolve swap coordinates with flag > config precedence. Returns `None` when
/// no venue is configured (token-mode). When a venue *is* named, the router and
/// usdc address are required and their absence errors here. The quoter is
/// passed through as `Option` — only the Uniswap venue needs it (Balancer
/// quotes through its router), so `from_config` enforces it per-venue.
///
/// Venue/address safety: if `--swap-venue` is passed and the config file names a
/// *different* venue, the config's address fields describe that other venue —
/// inheriting them would point the selected venue at foreign coordinates (e.g.
/// approving USDC to the wrong router). In that case venue-specific addresses
/// (router, quoter, both pool fields, fee tier) must come from flags; only the
/// venue-agnostic `usdc_address` still falls back to config.
pub fn resolve_swap(
    chain: &cli::ChainArgs,
    file: &FileConfig,
) -> anyhow::Result<Option<ResolvedSwap>> {
    let bc = file.blockchain.as_ref();
    let config_venue = bc.and_then(|b| b.swap_venue.clone());
    let venue = chain
        .swap_venue
        .map(|v| v.as_str().to_string())
        .or_else(|| config_venue.clone());
    let Some(venue) = venue else { return Ok(None) };

    // The venue is chosen by flag but the config names a different one, so the
    // config's venue-specific coordinates belong to that other venue and must
    // not be reused for the selected venue.
    let venue_overrides_config =
        chain.swap_venue.is_some() && config_venue.as_deref().is_some_and(|c| c != venue);

    // Venue-agnostic: the USDC token is the same regardless of venue.
    let pick = |flag: &Option<String>, cfg: fn(&FileBlockchain) -> Option<String>| {
        flag.clone().or_else(|| bc.and_then(cfg))
    };
    // Venue-specific: flag-only when the flag overrides a different config venue.
    let pick_venue = |flag: &Option<String>, cfg: fn(&FileBlockchain) -> Option<String>| {
        if venue_overrides_config {
            flag.clone()
        } else {
            flag.clone().or_else(|| bc.and_then(cfg))
        }
    };

    let router = pick_venue(&chain.swap_router_address, |b| {
        b.swap_router_address.clone()
    })
    .ok_or_else(|| {
        anyhow::anyhow!(
            "swap_router_address required for --swap-venue {venue} (pass --swap-router-address; \
                 the config file's address is for a different venue)"
        )
    })?;
    let quoter = pick_venue(&chain.swap_quoter_address, |b| {
        b.swap_quoter_address.clone()
    });
    let usdc = pick(&chain.usdc_address, |b| b.usdc_address.clone())
        .ok_or_else(|| anyhow::anyhow!("usdc_address required for --swap-venue {venue}"))?;
    let uniswap_fee_tier = if venue_overrides_config {
        chain.swap_fee_tier
    } else {
        chain
            .swap_fee_tier
            .or_else(|| bc.and_then(|b| b.swap_fee_tier))
    };
    Ok(Some(ResolvedSwap {
        venue,
        router,
        quoter,
        usdc,
        uniswap_fee_tier,
        balancer_pool_address: pick_venue(&chain.swap_balancer_pool, |b| {
            b.swap_balancer_pool.clone()
        }),
        pool: pick_venue(&chain.swap_pool_address, |b| b.swap_pool_address.clone()),
    }))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn empty_chain() -> cli::ChainArgs {
        cli::ChainArgs {
            common: cli::CommonChainArgs {
                config: None,
                rpc_url: None,
                chain_id: None,
                keystore: None,
                data_dir: Some(PathBuf::from("/tmp/decdn-test")),
                keystore_password_file: None,
                dry_run: true,
                json: false,
            },
            capacity_bond_address: None,
            swap_venue: None,
            swap_router_address: None,
            swap_quoter_address: None,
            usdc_address: None,
            swap_fee_tier: None,
            swap_balancer_pool: None,
            swap_pool_address: None,
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
        chain.common.rpc_url = Some("http://flag:8545".to_string());
        chain.capacity_bond_address = Some("0xFLAG".to_string());
        chain.common.chain_id = Some(99);
        let file = file_with(FileBlockchain {
            rpc_url: Some("http://config:8545".to_string()),
            chain_id: Some(1),
            capacity_bond_address: Some("0xCONFIG".to_string()),
            eth_keystore: None,
            publisher_registry_address: None,
            origin_assignment_address: None,
            swap_venue: None,
            swap_router_address: None,
            swap_quoter_address: None,
            usdc_address: None,
            swap_fee_tier: None,
            swap_balancer_pool: None,
            swap_pool_address: None,
            slash_appeal_address: None,
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
            publisher_registry_address: None,
            origin_assignment_address: None,
            swap_venue: None,
            swap_router_address: None,
            swap_quoter_address: None,
            usdc_address: None,
            swap_fee_tier: None,
            swap_balancer_pool: None,
            swap_pool_address: None,
            slash_appeal_address: None,
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
            publisher_registry_address: None,
            origin_assignment_address: None,
            swap_venue: None,
            swap_router_address: None,
            swap_quoter_address: None,
            usdc_address: None,
            swap_fee_tier: None,
            swap_balancer_pool: None,
            swap_pool_address: None,
            slash_appeal_address: None,
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

    /// Publish-side counterpart of [`empty_chain`], so the publish resolver's
    /// precedence can be exercised as thoroughly as the node resolver's — both
    /// now route through `resolve_common`, so a regression there hits both.
    fn empty_publish_chain() -> cli::PublishChainArgs {
        cli::PublishChainArgs {
            common: cli::CommonChainArgs {
                config: None,
                rpc_url: None,
                chain_id: None,
                keystore: None,
                data_dir: Some(PathBuf::from("/tmp/decdn-test")),
                keystore_password_file: None,
                dry_run: true,
                json: false,
            },
            publisher_registry_address: None,
            origin_assignment_address: None,
        }
    }

    /// `resolve_common` returns `(String, u64, PathBuf, PathBuf)` — `data_dir`
    /// and `keystore` are the same type, so transposing them at *any* of the
    /// three destructure sites compiles silently. The node path is guarded by
    /// `keystore_defaults_under_data_dir`; this is the publish path's guard.
    /// Without it, a swap confined to `resolve_publish` would ship green and
    /// hand the data dir to the keystore loader on every `decdn publish` submit.
    #[test]
    fn resolve_publish_keystore_defaults_under_data_dir() {
        let mut args = empty_publish_chain();
        args.common.rpc_url = Some("http://x".to_string());
        let r = resolve_publish(&args, &FileConfig::default()).unwrap();
        assert_eq!(r.data_dir, PathBuf::from("/tmp/decdn-test"));
        assert_eq!(r.keystore, PathBuf::from("/tmp/decdn-test/keystore.json"));
    }

    #[test]
    fn resolve_publish_config_fills_unset_flags() {
        let args = empty_publish_chain();
        let file = file_with(FileBlockchain {
            rpc_url: Some("http://config:8545".to_string()),
            chain_id: None,
            eth_keystore: Some(PathBuf::from("/keys/ks.json")),
            publisher_registry_address: Some("0xCONFIG".to_string()),
            origin_assignment_address: Some("0xOA".to_string()),
            ..Default::default()
        });
        let r = resolve_publish(&args, &file).unwrap();
        assert_eq!(r.rpc_url, "http://config:8545");
        // `chain_id` absent from both flag and config → the shared default.
        assert_eq!(r.chain_id, DEFAULT_CHAIN_ID);
        assert_eq!(r.keystore, PathBuf::from("/keys/ks.json"));
        assert_eq!(r.publisher_registry_address.as_deref(), Some("0xCONFIG"));
        assert_eq!(r.origin_assignment_address.as_deref(), Some("0xOA"));
    }

    #[test]
    fn resolve_publish_missing_rpc_url_errors() {
        let args = empty_publish_chain();
        let err = resolve_publish(&args, &FileConfig::default()).unwrap_err();
        assert!(err.to_string().contains("rpc_url not set"), "{err}");
    }

    #[test]
    fn resolve_publish_flag_beats_config() {
        let args = cli::PublishChainArgs {
            common: cli::CommonChainArgs {
                config: None,
                rpc_url: Some("http://flag:8545".to_string()),
                chain_id: Some(42),
                keystore: None,
                data_dir: Some(PathBuf::from("/tmp/decdn-test")),
                keystore_password_file: None,
                dry_run: true,
                json: false,
            },
            publisher_registry_address: Some("0xFLAG".to_string()),
            origin_assignment_address: None,
        };
        let file = file_with(FileBlockchain {
            rpc_url: Some("http://config:8545".to_string()),
            chain_id: Some(1),
            capacity_bond_address: None,
            eth_keystore: None,
            publisher_registry_address: Some("0xCONFIG".to_string()),
            origin_assignment_address: Some("0xOA".to_string()),
            swap_venue: None,
            swap_router_address: None,
            swap_quoter_address: None,
            usdc_address: None,
            swap_fee_tier: None,
            swap_balancer_pool: None,
            swap_pool_address: None,
            slash_appeal_address: None,
        });
        let r = resolve_publish(&args, &file).unwrap();
        assert_eq!(r.rpc_url, "http://flag:8545");
        assert_eq!(r.chain_id, 42);
        assert_eq!(r.publisher_registry_address.as_deref(), Some("0xFLAG"));
        // unset flag falls through to config
        assert_eq!(r.origin_assignment_address.as_deref(), Some("0xOA"));
    }

    #[test]
    fn resolve_swap_none_when_unset() {
        let chain = empty_chain();
        assert!(
            resolve_swap(&chain, &FileConfig::default())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn resolve_swap_flag_beats_config() {
        let mut chain = empty_chain();
        chain.swap_venue = Some(cli::SwapVenueArg::UniswapV3);
        chain.swap_router_address = Some("0xROUTER".into());
        chain.swap_quoter_address = Some("0xQUOTER".into());
        chain.usdc_address = Some("0xUSDC".into());
        chain.swap_fee_tier = Some(3000);
        let s = resolve_swap(&chain, &FileConfig::default())
            .unwrap()
            .unwrap();
        assert_eq!(s.venue, "uniswap-v3");
        assert_eq!(s.router, "0xROUTER");
        assert_eq!(s.uniswap_fee_tier, Some(3000));
    }

    #[test]
    fn resolve_swap_balancer_needs_no_quoter() {
        // Balancer quotes through its router, so a quoter is not required —
        // `resolve_swap` must succeed with `quoter: None` and carry the pool.
        let mut chain = empty_chain();
        chain.swap_venue = Some(cli::SwapVenueArg::BalancerV3);
        chain.swap_router_address = Some("0xROUTER".into());
        chain.usdc_address = Some("0xUSDC".into());
        chain.swap_balancer_pool = Some("0xBALPOOL".into());
        let s = resolve_swap(&chain, &FileConfig::default())
            .unwrap()
            .unwrap();
        assert_eq!(s.venue, "balancer-v3");
        assert!(s.quoter.is_none());
        assert_eq!(s.balancer_pool_address.as_deref(), Some("0xBALPOOL"));
    }

    #[test]
    fn resolve_swap_carries_pool_flag_beats_config() {
        let mut chain = empty_chain();
        chain.swap_venue = Some(cli::SwapVenueArg::UniswapV3);
        chain.swap_router_address = Some("0xROUTER".into());
        chain.swap_quoter_address = Some("0xQUOTER".into());
        chain.usdc_address = Some("0xUSDC".into());
        chain.swap_pool_address = Some("0xPOOLFLAG".into());
        let file = file_with(FileBlockchain {
            rpc_url: Some("http://config:8545".to_string()),
            chain_id: None,
            capacity_bond_address: None,
            eth_keystore: None,
            publisher_registry_address: None,
            origin_assignment_address: None,
            swap_venue: None,
            swap_router_address: None,
            swap_quoter_address: None,
            usdc_address: None,
            swap_fee_tier: None,
            swap_balancer_pool: None,
            swap_pool_address: Some("0xPOOLCONFIG".to_string()),
            slash_appeal_address: None,
        });
        let s = resolve_swap(&chain, &file).unwrap().unwrap();
        // Flag wins over config for the pool address.
        assert_eq!(s.pool.as_deref(), Some("0xPOOLFLAG"));
    }

    #[test]
    fn resolve_swap_pool_falls_through_to_config() {
        let mut chain = empty_chain();
        chain.swap_venue = Some(cli::SwapVenueArg::UniswapV3);
        chain.swap_router_address = Some("0xROUTER".into());
        chain.swap_quoter_address = Some("0xQUOTER".into());
        chain.usdc_address = Some("0xUSDC".into());
        let file = file_with(FileBlockchain {
            rpc_url: None,
            chain_id: None,
            capacity_bond_address: None,
            eth_keystore: None,
            publisher_registry_address: None,
            origin_assignment_address: None,
            swap_venue: None,
            swap_router_address: None,
            swap_quoter_address: None,
            usdc_address: None,
            swap_fee_tier: None,
            swap_balancer_pool: None,
            swap_pool_address: Some("0xPOOLCONFIG".to_string()),
            slash_appeal_address: None,
        });
        let s = resolve_swap(&chain, &file).unwrap().unwrap();
        assert_eq!(s.pool.as_deref(), Some("0xPOOLCONFIG"));
    }

    #[test]
    fn resolve_swap_venue_override_rejects_config_router() {
        // Config names balancer-v3 with a Balancer router; the operator forces
        // --swap-venue uniswap-v3. The Balancer router must NOT be inherited for
        // the Uniswap venue (that would approve USDC to the wrong contract) — a
        // uniswap router flag is required instead.
        let mut chain = empty_chain();
        chain.swap_venue = Some(cli::SwapVenueArg::UniswapV3);
        chain.usdc_address = Some("0xUSDC".into());
        let file = file_with(FileBlockchain {
            swap_venue: Some("balancer-v3".to_string()),
            swap_router_address: Some("0xBALANCER_ROUTER".to_string()),
            swap_balancer_pool: Some("0xBALPOOL".to_string()),
            ..Default::default()
        });
        let err = resolve_swap(&chain, &file).unwrap_err().to_string();
        assert!(
            err.contains("swap_router_address required"),
            "expected a router-required error, got: {err}"
        );
    }

    #[test]
    fn resolve_swap_venue_override_uses_flag_addresses() {
        // Same venue mismatch, but the operator supplies the Uniswap coordinates
        // by flag — resolution succeeds and never picks up the config's Balancer
        // router / pool.
        let mut chain = empty_chain();
        chain.swap_venue = Some(cli::SwapVenueArg::UniswapV3);
        chain.swap_router_address = Some("0xUNI_ROUTER".into());
        chain.swap_quoter_address = Some("0xUNI_QUOTER".into());
        chain.usdc_address = Some("0xUSDC".into());
        chain.swap_fee_tier = Some(3000);
        let file = file_with(FileBlockchain {
            swap_venue: Some("balancer-v3".to_string()),
            swap_router_address: Some("0xBALANCER_ROUTER".to_string()),
            swap_balancer_pool: Some("0xBALPOOL".to_string()),
            swap_pool_address: Some("0xBAL_STALE_POOL".to_string()),
            ..Default::default()
        });
        let s = resolve_swap(&chain, &file).unwrap().unwrap();
        assert_eq!(s.venue, "uniswap-v3");
        assert_eq!(s.router, "0xUNI_ROUTER");
        // The config's Balancer-venue pool must not leak into the Uniswap venue.
        assert!(s.pool.is_none());
        assert!(s.balancer_pool_address.is_none());
    }

    #[test]
    fn resolve_swap_matching_venue_still_inherits_config() {
        // When the flag venue matches the config venue, the config's
        // venue-specific addresses are still inherited (no mismatch).
        let mut chain = empty_chain();
        chain.swap_venue = Some(cli::SwapVenueArg::UniswapV3);
        let file = file_with(FileBlockchain {
            swap_venue: Some("uniswap-v3".to_string()),
            swap_router_address: Some("0xUNI_ROUTER".to_string()),
            swap_quoter_address: Some("0xUNI_QUOTER".to_string()),
            usdc_address: Some("0xUSDC".to_string()),
            swap_fee_tier: Some(500),
            swap_pool_address: Some("0xUNI_POOL".to_string()),
            ..Default::default()
        });
        let s = resolve_swap(&chain, &file).unwrap().unwrap();
        assert_eq!(s.router, "0xUNI_ROUTER");
        assert_eq!(s.pool.as_deref(), Some("0xUNI_POOL"));
        assert_eq!(s.uniswap_fee_tier, Some(500));
    }
}
