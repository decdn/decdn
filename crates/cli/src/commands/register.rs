//! `decdn node register` — on-chain node registration (ADR 019 § Step 2.3).
//!
//! Unlike the other `node` subcommands (which talk to a running node's
//! loopback admin RPC), this performs Phase 2 Step 2.3 of operator
//! onboarding: it loads the local iroh node key and Ethereum keystore,
//! reads the operator's on-chain nonces, builds the EIP-712
//! `bindingSignature` and the ed25519 ownership signature locally, and
//! submits `CapacityBond.registerNode`.
//!
//! Phase 2.1/2.2 (`approve` + `bond` + `declareMbps`) are a precondition:
//! the contract reverts `BondBelowMinimum` / `BondBelowCurve` if the bond
//! is not already posted. `--dry-run` builds and prints everything (it
//! still reads the chain for the nonces the signatures depend on) but does
//! not submit.

use std::io;
use std::path::{Path, PathBuf};

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, B256, Bytes};
use alloy::providers::ProviderBuilder;
use alloy::signers::SignerSync;
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use decdn_common::cli;
use decdn_common::cli::common::expand_tilde;
use decdn_common::config::DEFAULT_CHAIN_ID;
use decdn_common::identity;
use decdn_incentive::capacity_bond::CapacityBond;
use decdn_incentive::eth_identity::{self, PasswordSource};
use decdn_incentive::{bind_sig, node_register};
use serde::Deserialize;

/// Partial deserializer for the TOML config — only the `[blockchain]` and
/// `[identity]` fields `register` needs. Deliberately partial (like
/// `node.rs`'s `AdminPortConfig`) so an operator's typo in an unrelated
/// section can't block registration; serde-toml ignores unknown fields.
#[derive(Debug, Default, Deserialize)]
struct RegisterFileConfig {
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
struct Resolved {
    rpc_url: String,
    chain_id: u64,
    capacity_bond_address: String,
    keystore: PathBuf,
    data_dir: PathBuf,
}

/// Entry point for `decdn node register`.
pub async fn run(args: &cli::RegisterArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    let config_path = args.config.as_deref().or(global_config);
    let file = load_optional_config(config_path)?;
    let resolved = resolve(args, &file)?;

    let cb_addr: Address = resolved.capacity_bond_address.parse().with_context(|| {
        format!(
            "capacity_bond_address {:?} is not a valid address",
            resolved.capacity_bond_address
        )
    })?;
    // The `Url` type is inferred from `connect_http`'s parameter below; naming
    // it explicitly would need `alloy::transports`, which is not exposed under
    // this crate's alloy feature set.
    let rpc_url = resolved.rpc_url.clone();

    // Load the iroh node key. Require it to already exist — `load_or_generate`
    // would otherwise mint a *fresh* identity and register that, silently
    // diverging from the key the daemon serves under. Operators create it with
    // `decdn key-gen`.
    anyhow::ensure!(
        identity::key_path(&resolved.data_dir).exists(),
        "no node key at {}; run `decdn key-gen` first (register binds the existing iroh identity)",
        identity::key_path(&resolved.data_dir).display(),
    );
    let node_secret = identity::load_or_generate(&resolved.data_dir).with_context(|| {
        format!(
            "failed to load node key from {}",
            resolved.data_dir.display()
        )
    })?;
    let node_id = B256::from_slice(node_secret.public().as_bytes());

    let signer =
        load_operator_signer(resolved_password_file(args), resolved.keystore.clone()).await?;
    let operator = signer.address();

    let provider = ProviderBuilder::new()
        .wallet(EthereumWallet::from(signer.clone()))
        .connect_http(
            rpc_url
                .parse()
                .with_context(|| format!("rpc_url {rpc_url:?} is not a valid URL"))?,
        );
    let bond = CapacityBond::new(cb_addr, &provider);

    // Nonces feed both signature digests, so they are read even on a dry run.
    let binding_nonce: u64 =
        bond.bindingNonce(operator).call().await.with_context(|| {
            format!("failed to read bindingNonce from CapacityBond at {cb_addr}")
        })?;
    let registration_nonce: u64 =
        bond.registrationNonce(node_id)
            .call()
            .await
            .with_context(|| {
                format!("failed to read registrationNonce from CapacityBond at {cb_addr}")
            })?;

    // EIP-712 BindNodeId signature (Ethereum key). `sign_hash_sync` yields a
    // low-s, 27/28-`v` 65-byte signature accepted by the on-chain OZ
    // `SignatureChecker` (same path as `decdn_incentive::bind_sig`).
    let domain = bind_sig::bind_node_id_domain(resolved.chain_id, cb_addr);
    let bind_hash = bind_sig::binding_signing_hash(node_id, binding_nonce, &domain);
    let binding_sig = signer.sign_hash_sync(&bind_hash)?.as_bytes().to_vec();

    // ed25519 ownership signature (iroh node key) over the contract's
    // ownership digest.
    let digest = node_register::ownership_message_digest(
        node_id,
        operator,
        resolved.chain_id,
        registration_nonce,
    );
    let ed25519_sig = node_secret.sign(digest.as_slice()).to_bytes().to_vec();

    let multiaddrs = node_register::pack_multiaddrs(&args.multiaddrs)?;

    let params = Params {
        node_id,
        operator,
        chain_id: resolved.chain_id,
        capacity_bond: cb_addr,
        region: &args.region,
        binding_nonce,
        registration_nonce,
        multiaddr_count: args.multiaddrs.len(),
        binding_sig: &binding_sig,
        ed25519_sig: &ed25519_sig,
    };

    if args.dry_run {
        let mut out = io::stdout().lock();
        write_params(&mut out, &params, args.json, None)
            .context("failed to write dry-run output")?;
        return Ok(());
    }

    let pending = bond
        .registerNode(
            node_id,
            Bytes::from(multiaddrs),
            args.region.clone(),
            Bytes::from(binding_sig.clone()),
            Bytes::from(ed25519_sig.clone()),
        )
        .send()
        .await
        .context("registerNode transaction failed to send (check RPC, gas, and that the bond covers minBond / the declared-capacity curve)")?;
    let receipt = pending
        .get_receipt()
        .await
        .context("registerNode sent but the receipt could not be fetched")?;

    let tx = receipt.transaction_hash;
    anyhow::ensure!(
        receipt.status(),
        "registerNode reverted (tx {tx}); most likely the bond does not cover minBond / the \
         declared-capacity curve, the nodeId/address is already bound, or a signature was rejected",
    );

    let mut out = io::stdout().lock();
    write_params(&mut out, &params, args.json, Some(tx)).context("failed to write result")?;
    Ok(())
}

/// Registration parameters, for printing in both dry-run and post-submit paths.
struct Params<'a> {
    node_id: B256,
    operator: Address,
    chain_id: u64,
    capacity_bond: Address,
    region: &'a str,
    binding_nonce: u64,
    registration_nonce: u64,
    multiaddr_count: usize,
    binding_sig: &'a [u8],
    ed25519_sig: &'a [u8],
}

/// Write the parameters as JSON or grep-friendly `key=value` lines. `tx` is
/// `Some` after a successful submit, `None` for a dry run. Pure (`&mut impl
/// Write`) so the output shape is unit-testable without a chain.
fn write_params(
    w: &mut impl io::Write,
    p: &Params<'_>,
    json: bool,
    tx: Option<B256>,
) -> io::Result<()> {
    if json {
        let value = serde_json::json!({
            "submitted": tx.is_some(),
            "tx": tx.map(|h| format!("{h:#x}")),
            "node_id": format!("{:#x}", p.node_id),
            "operator": format!("{:#x}", p.operator),
            "chain_id": p.chain_id,
            "capacity_bond": format!("{:#x}", p.capacity_bond),
            "region": p.region,
            "binding_nonce": p.binding_nonce,
            "registration_nonce": p.registration_nonce,
            "multiaddrs": p.multiaddr_count,
            "binding_sig": format!("0x{}", hex_encode(p.binding_sig)),
            "ed25519_sig": format!("0x{}", hex_encode(p.ed25519_sig)),
        });
        return writeln!(w, "{value}");
    }
    writeln!(w, "node_id={:#x}", p.node_id)?;
    writeln!(w, "operator={:#x}", p.operator)?;
    writeln!(w, "chain_id={}", p.chain_id)?;
    writeln!(w, "capacity_bond={:#x}", p.capacity_bond)?;
    writeln!(w, "region={}", p.region)?;
    writeln!(w, "binding_nonce={}", p.binding_nonce)?;
    writeln!(w, "registration_nonce={}", p.registration_nonce)?;
    writeln!(w, "multiaddrs={}", p.multiaddr_count)?;
    writeln!(w, "binding_sig=0x{}", hex_encode(p.binding_sig))?;
    writeln!(w, "ed25519_sig=0x{}", hex_encode(p.ed25519_sig))?;
    match tx {
        Some(h) => writeln!(w, "submitted=true tx={h:#x}"),
        None => writeln!(w, "submitted=false dry_run=true"),
    }
}

/// Lowercase hex without a dependency on a hex crate in this binary.
fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Keystore password file: explicit flag wins; otherwise unset (env / prompt
/// handle the rest). Split out so `run` reads top-to-bottom.
fn resolved_password_file(args: &cli::RegisterArgs) -> Option<PathBuf> {
    args.keystore_password_file.as_deref().map(expand_tilde)
}

/// Load the operator's Ethereum keystore signer, sourcing the password from
/// the `DECDN_KEYSTORE_PASSWORD` env var, then `--keystore-password-file`,
/// then an interactive prompt — the same precedence the daemon uses.
///
/// `load_signer` runs the keystore's scrypt KDF, which is CPU-heavy
/// (hundreds of ms); it is offloaded to `spawn_blocking` so it doesn't stall
/// the async executor, matching `decdn-node`'s runtime keystore load.
async fn load_operator_signer(
    password_file: Option<PathBuf>,
    keystore: PathBuf,
) -> anyhow::Result<PrivateKeySigner> {
    let mut sources = vec![PasswordSource::Env(eth_identity::KEYSTORE_PASSWORD_ENV)];
    if let Some(path) = password_file {
        sources.push(PasswordSource::File(path));
    }
    sources.push(PasswordSource::Prompt { confirm: false });
    let password = eth_identity::read_password(&sources, "eth keystore password")?;
    let display = keystore.display().to_string();
    tokio::task::spawn_blocking(move || eth_identity::load_signer(&keystore, &password))
        .await
        .context("keystore decryption task panicked")?
        .with_context(|| format!("failed to load keystore at {display}"))
}

/// Load the partial config from `config_path`. An explicit path that is
/// missing/unreadable is an error; the default path simply falls through to an
/// empty config (every field can also come from a flag).
fn load_optional_config(config_path: Option<&Path>) -> anyhow::Result<RegisterFileConfig> {
    let (path, explicit) = match config_path {
        Some(p) => (expand_tilde(p), true),
        None => match cli::common::default_config_path() {
            Some(p) => (p, false),
            None => return Ok(RegisterFileConfig::default()),
        },
    };
    match std::fs::read_to_string(&path) {
        Ok(contents) => toml::from_str(&contents)
            .with_context(|| format!("failed to parse config file {}", path.display())),
        Err(e) if e.kind() == io::ErrorKind::NotFound && !explicit => {
            Ok(RegisterFileConfig::default())
        }
        Err(e) => {
            Err(anyhow::Error::new(e)
                .context(format!("failed to read config file {}", path.display())))
        }
    }
}

/// Resolve every coordinate with flag > config > default precedence. Pure so
/// the precedence is unit-testable.
fn resolve(args: &cli::RegisterArgs, file: &RegisterFileConfig) -> anyhow::Result<Resolved> {
    let bc = file.blockchain.as_ref();
    let rpc_url = args
        .rpc_url
        .clone()
        .or_else(|| bc.and_then(|b| b.rpc_url.clone()))
        .ok_or_else(|| {
            anyhow::anyhow!("rpc_url not set (pass --rpc-url or set blockchain.rpc_url)")
        })?;
    let capacity_bond_address = args
        .capacity_bond_address
        .clone()
        .or_else(|| bc.and_then(|b| b.capacity_bond_address.clone()))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "capacity_bond_address not set (pass --capacity-bond-address or set \
                 blockchain.capacity_bond_address)"
            )
        })?;
    let chain_id = args
        .chain_id
        .or_else(|| bc.and_then(|b| b.chain_id))
        .unwrap_or(DEFAULT_CHAIN_ID);
    // `expand_tilde` is applied to whichever explicit value wins (flag OR
    // config) — config-file paths get `~` expansion too, not just flags. The
    // `default_data_dir` fallback is already absolute.
    let data_dir = args
        .data_dir
        .clone()
        .or_else(|| file.identity.as_ref().and_then(|i| i.data_dir.clone()))
        .map(|p| expand_tilde(&p))
        .or_else(cli::default_data_dir)
        .ok_or_else(|| {
            anyhow::anyhow!("data_dir not set and no default available (pass --data-dir)")
        })?;
    let keystore = args
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn base_args() -> cli::RegisterArgs {
        cli::RegisterArgs {
            region: "DE".to_string(),
            multiaddrs: vec![],
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

    #[test]
    fn flags_override_config() {
        let mut args = base_args();
        args.rpc_url = Some("http://flag:8545".to_string());
        args.capacity_bond_address = Some("0xFLAG".to_string());
        args.chain_id = Some(99);
        let file = RegisterFileConfig {
            blockchain: Some(FileBlockchain {
                rpc_url: Some("http://config:8545".to_string()),
                chain_id: Some(1),
                capacity_bond_address: Some("0xCONFIG".to_string()),
                eth_keystore: None,
            }),
            identity: None,
        };
        let r = resolve(&args, &file).unwrap();
        assert_eq!(r.rpc_url, "http://flag:8545");
        assert_eq!(r.capacity_bond_address, "0xFLAG");
        assert_eq!(r.chain_id, 99);
    }

    #[test]
    fn config_fills_unset_flags() {
        let args = base_args();
        let file = RegisterFileConfig {
            blockchain: Some(FileBlockchain {
                rpc_url: Some("http://config:8545".to_string()),
                chain_id: None,
                capacity_bond_address: Some("0xCONFIG".to_string()),
                eth_keystore: Some(PathBuf::from("/keys/ks.json")),
            }),
            identity: None,
        };
        let r = resolve(&args, &file).unwrap();
        assert_eq!(r.rpc_url, "http://config:8545");
        assert_eq!(r.capacity_bond_address, "0xCONFIG");
        // chain_id absent everywhere → default.
        assert_eq!(r.chain_id, DEFAULT_CHAIN_ID);
        assert_eq!(r.keystore, PathBuf::from("/keys/ks.json"));
    }

    #[test]
    fn keystore_defaults_under_data_dir() {
        let args = base_args();
        let file = RegisterFileConfig {
            blockchain: Some(FileBlockchain {
                rpc_url: Some("http://x".to_string()),
                chain_id: None,
                capacity_bond_address: Some("0xY".to_string()),
                eth_keystore: None,
            }),
            identity: None,
        };
        let r = resolve(&args, &file).unwrap();
        assert_eq!(r.keystore, PathBuf::from("/tmp/decdn-test/keystore.json"));
    }

    #[test]
    fn missing_required_fields_error() {
        let args = base_args();
        let err = resolve(&args, &RegisterFileConfig::default()).unwrap_err();
        assert!(err.to_string().contains("rpc_url not set"), "{err}");
    }

    #[test]
    fn dry_run_output_has_signatures() {
        let p = Params {
            node_id: B256::repeat_byte(0xAB),
            operator: Address::repeat_byte(0xCD),
            chain_id: 31337,
            capacity_bond: Address::repeat_byte(0x01),
            region: "DE",
            binding_nonce: 0,
            registration_nonce: 0,
            multiaddr_count: 1,
            binding_sig: &[0x11; 65],
            ed25519_sig: &[0x22; 64],
        };
        let mut buf = Vec::new();
        write_params(&mut buf, &p, false, None).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("submitted=false dry_run=true"), "{s}");
        assert!(s.contains("ed25519_sig=0x2222"), "{s}");
        assert!(s.contains("region=DE"), "{s}");
    }
}
