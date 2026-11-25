//! `decdn node register` — on-chain node registration (ADR 019).
//!
//! Unlike the other `node` subcommands (which talk to a running node's
//! loopback admin RPC), this performs the on-chain operator-registration
//! step of onboarding: it loads the local iroh node key and Ethereum keystore,
//! reads the nodeId's registration nonce, builds the ed25519 ownership
//! signature locally, and submits `CapacityBond.registerNode` from the
//! operator's Ethereum key.
//!
//! Bonding (`approve` + `bond` + `declareMbps`) is a precondition:
//! the contract reverts `BondBelowMinimum` / `BondBelowCurve` if the bond
//! is not already posted (run `decdn node bond` first). `--dry-run` builds
//! and prints everything but does not submit. It still reads the chain: the
//! `maxMultiaddrSize` ceiling, so an oversized multiaddr set fails a dry run as
//! it would a live one, and the nonce the signature depends on.
//!
//! The signing + submit path is factored into `submit_registration` so
//! `decdn setup` (#933) can drive registration with the same already-loaded
//! signer + provider it used for bonding, decrypting the keystore once.

use std::io;
use std::path::Path;

use alloy::primitives::{Address, B256, Bytes};
use alloy::providers::Provider;
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use decdn_common::cli;
use decdn_common::identity;
use decdn_incentive::capacity_bond::CapacityBond;
use decdn_incentive::node_register;

use crate::commands::{chain_ctx, terms, update_multiaddrs};

/// Entry point for `decdn node register`.
pub async fn run(args: &cli::RegisterArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    let config_path = args.chain.common.config.as_deref().or(global_config);
    let file = chain_ctx::load_optional_config(config_path)?;
    let resolved = chain_ctx::resolve(&args.chain, &file)?;
    let cb_addr = resolved.capacity_bond_address;
    // Chosen before the signer loads, so a bad `network.external_addrs` entry
    // fails before any password prompt.
    let multiaddrs = registration_multiaddrs(&args.multiaddrs, &file, None)?;

    let signer = chain_ctx::load_operator_signer(&args.chain.common, &resolved.keystore).await?;
    let provider = decdn_client::provider::build_provider(&resolved.rpc_url, &signer)?;
    let bond = CapacityBond::new(cb_addr, &provider);

    // An oversized set fails here, before the terms prompt, and on `--dry-run`
    // too: the signatures a dry run prints would only revert on submission.
    ensure_multiaddrs_fit(&bond, cb_addr, &multiaddrs).await?;

    // ADR 019 § Terms Acceptance — read the network's current terms hash, then
    // require the operator to accept the matching embedded terms before
    // registering. Enforced on `--dry-run` too, so a dry run never shows a
    // registration over terms the operator hasn't seen.
    let terms_hash = bond.currentTermsHash().call().await.with_context(|| {
        format!("failed to read currentTermsHash from CapacityBond at {cb_addr}")
    })?;
    terms::ensure_accepted_async(terms_hash, args.accept_terms).await?;

    // `decdn node register` submits a single transaction and has no partial
    // report to print, so the slot is only consulted through the error message.
    let mut tx_slot = None;
    let outcome = submit_registration(
        &provider,
        &signer,
        &resolved.data_dir,
        cb_addr,
        resolved.chain_id,
        &args.region,
        &multiaddrs,
        terms_hash,
        args.chain.common.dry_run,
        &mut tx_slot,
    )
    .await?;

    let mut out = io::stdout().lock();
    let label = if outcome.tx.is_some() {
        "failed to write result"
    } else {
        "failed to write dry-run output"
    };
    write_outcome(&mut out, &outcome, args.chain.common.json).context(label)?;
    Ok(())
}

/// Refuse a multiaddr set whose packed form exceeds `CapacityBond.maxMultiaddrSize`,
/// so an oversized set fails as a named error before the terms prompt and any
/// transaction, not as a `registerNode` revert. Packs with
/// [`node_register::pack_multiaddrs`] before the chain read, so a single entry
/// past the `u16` length prefix also fails here. An empty set (a host behind
/// NAT) packs to 0 bytes and is always within the ceiling.
///
/// # Errors
///
/// Returns an error when an entry does not pack, the ceiling read fails, or the
/// packed set is over the ceiling.
pub(crate) async fn ensure_multiaddrs_fit<P: Provider>(
    bond: &CapacityBond::CapacityBondInstance<P>,
    cb_addr: Address,
    multiaddrs: &[String],
) -> anyhow::Result<()> {
    let packed = node_register::pack_multiaddrs(multiaddrs)?;
    let max_multiaddr_size = bond.maxMultiaddrSize().call().await.with_context(|| {
        format!("failed to read maxMultiaddrSize from CapacityBond at {cb_addr}")
    })?;
    update_multiaddrs::ensure_packed_within(packed.len(), max_multiaddr_size, "registerNode")
}

/// The multiaddrs to register (ADR 001 § Node Discovery), first match wins:
///
/// 1. `explicit`, when the operator passed any. `network.external_addrs` is
///    then neither read nor checked.
/// 2. Each `network.external_addrs` address, on its own port, when that list
///    holds any address. An empty list counts as unset. The configured
///    addresses replace the host's own, as in the daemon's reachability check
///    (see [`decdn_common::net::public_quic_addrs`]).
/// 3. One `/ip{4,6}/<ip>/udp/<bind_port>/quic-v1` per public address on the
///    host's default route. A host behind NAT gets none, since its route
///    source is not dialable; its operator sets `network.external_addrs` or
///    passes the forwarded address with `--multiaddr` instead.
///
/// `external` is the list [`chain_ctx::FileConfig::external_addrs`] already
/// returned for this `file`, when the caller parsed it earlier; `None` parses
/// it here. It is ignored when `explicit` is non-empty.
///
/// The chosen default is reported on stderr, so `--json` stdout stays
/// machine-readable. Each note names the config file it read; a fallback note
/// instead names the default path where no file was found, or says there is
/// no config file when no default path exists.
///
/// # Errors
///
/// Returns an error that names every invalid `network.external_addrs` entry,
/// when no `explicit` multiaddr is given.
pub(crate) fn registration_multiaddrs(
    explicit: &[String],
    file: &chain_ctx::FileConfig,
    external: Option<Vec<std::net::SocketAddr>>,
) -> anyhow::Result<Vec<String>> {
    if !explicit.is_empty() {
        return Ok(explicit.to_vec());
    }
    let external = match external {
        Some(external) => external,
        None => file.external_addrs()?,
    };
    if !external.is_empty() {
        let chosen: Vec<String> = external
            .iter()
            .map(|a| decdn_common::net::quic_multiaddr(a.ip(), a.port()))
            .collect();
        eprintln!(
            "note: no --multiaddr given; registering network.external_addrs{}: {}",
            read_from(file.source()),
            chosen.join(", ")
        );
        return Ok(chosen);
    }
    let bind_port = file.bind_port();
    let chosen = default_multiaddrs(&decdn_common::net::route_public_ips(), bind_port);
    if chosen.is_empty() {
        eprintln!(
            "note: no --multiaddr given ({}) and no public address on the default route \
             (behind NAT); registering none. Peers reach the node through the relay and its pkarr \
             record. If UDP {bind_port} is forwarded to this host, set the forwarded address \
             in network.external_addrs or pass it with --multiaddr.",
            no_external_addrs(file.source()),
        );
    } else {
        eprintln!(
            "note: no --multiaddr given ({}); registering this host's public address: {}",
            no_external_addrs(file.source()),
            chosen.join(", ")
        );
    }
    Ok(chosen)
}

/// `" from <path>"` when the config was read from a file, else nothing: the
/// suffix the configured-list note puts after `network.external_addrs`.
fn read_from(source: &chain_ctx::ConfigSource) -> String {
    match source {
        chain_ctx::ConfigSource::Read(path) => format!(" from {}", path.display()),
        chain_ctx::ConfigSource::NoPath | chain_ctx::ConfigSource::Missing(_) => String::new(),
    }
}

/// Why no `network.external_addrs` list applies: the file read sets none, or
/// no config file was found. The fallback notes carry it, so an operator sees
/// which config the command looked at.
fn no_external_addrs(source: &chain_ctx::ConfigSource) -> String {
    match source {
        chain_ctx::ConfigSource::Read(path) => {
            format!("{} sets no network.external_addrs address", path.display())
        }
        chain_ctx::ConfigSource::Missing(path) => {
            format!("no config file at {}", path.display())
        }
        chain_ctx::ConfigSource::NoPath => "no config file".to_string(),
    }
}

fn default_multiaddrs(public: &[std::net::IpAddr], bind_port: u16) -> Vec<String> {
    public
        .iter()
        .map(|ip| decdn_common::net::quic_multiaddr(*ip, bind_port))
        .collect()
}

/// Registration parameters + result, owned so the formatter and `decdn setup`
/// can read them after the signer/provider go out of scope. `tx` is `Some`
/// after a successful submit, `None` for a dry run.
pub(crate) struct RegisterOutcome {
    pub(crate) node_id: B256,
    pub(crate) operator: Address,
    pub(crate) chain_id: u64,
    pub(crate) capacity_bond: Address,
    pub(crate) region: String,
    pub(crate) registration_nonce: u64,
    pub(crate) multiaddr_count: usize,
    pub(crate) ed25519_sig: Vec<u8>,
    pub(crate) tx: Option<B256>,
}

/// Build the ownership signature and submit `registerNode` (ADR 019) against
/// an already-built `provider` and `signer`. `dry_run` reads the chain for the
/// nonce the signature depends on but does not submit. Shared by `run` and
/// `decdn setup` so the keystore is decrypted once across bond + register.
/// Callers check `multiaddrs` with [`ensure_multiaddrs_fit`] before any
/// prompt; this function does not re-check the ceiling.
///
/// `tx_slot` is caller-owned for the same reason `bond::execute`'s `Outcome` is
/// (#1355): on an unreadable receipt the transaction is broadcast and may take
/// effect, and the returned [`RegisterOutcome`] is gone with the `Err`. See
/// [`decdn_incentive::tx::send`] for the slot's contract.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn submit_registration<P: Provider + Clone>(
    provider: &P,
    signer: &PrivateKeySigner,
    data_dir: &Path,
    cb_addr: Address,
    chain_id: u64,
    region: &str,
    multiaddrs: &[String],
    terms_hash: B256,
    dry_run: bool,
    tx_slot: &mut Option<B256>,
) -> anyhow::Result<RegisterOutcome> {
    // Load the iroh node key. Require it to already exist — `load_or_generate`
    // would otherwise mint a *fresh* identity and register that, silently
    // diverging from the key the daemon serves under. Operators create it with
    // `decdn key-gen` (or `decdn setup`).
    let key_path = identity::key_path(data_dir);
    anyhow::ensure!(
        key_path.exists(),
        "no node key at {}; run `decdn key-gen` first (register binds the existing iroh identity)",
        key_path.display(),
    );
    let node_secret = identity::load_or_generate(data_dir)
        .with_context(|| format!("failed to load node key from {}", data_dir.display()))?;
    let node_id = B256::from_slice(node_secret.public().as_bytes());

    let operator = signer.address();
    let bond = CapacityBond::new(cb_addr, provider);

    // The nonce feeds the ownership digest, so it is read even on a dry run.
    let registration_nonce: u64 =
        bond.registrationNonce(node_id)
            .call()
            .await
            .with_context(|| {
                format!("failed to read registrationNonce from CapacityBond at {cb_addr}")
            })?;

    // ADR 019 § Terms Acceptance — `terms_hash` is the accepted (and staleness-
    // checked) `currentTermsHash` the caller obtained via `terms::ensure_accepted`.
    // The operator's transaction carries it, and the on-chain contract enforces
    // `termsHash == currentTermsHash`.

    // ed25519 ownership signature (iroh node key) over the contract's
    // ownership digest.
    let digest =
        node_register::ownership_message_digest(node_id, operator, chain_id, registration_nonce);
    let ed25519_sig = node_secret.sign(digest.as_slice()).to_bytes().to_vec();

    let packed_multiaddrs = node_register::pack_multiaddrs(multiaddrs)?;

    let mut outcome = RegisterOutcome {
        node_id,
        operator,
        chain_id,
        capacity_bond: cb_addr,
        region: region.to_string(),
        registration_nonce,
        multiaddr_count: multiaddrs.len(),
        ed25519_sig: ed25519_sig.clone(),
        tx: None,
    };

    if dry_run {
        return Ok(outcome);
    }

    // Via the shared `send` (#1355) so a receipt-fetch timeout still names the
    // in-flight tx, into the CALLER's `tx_slot`. Writing it into the local
    // `outcome` instead would not survive: `outcome` is returned by value, so
    // the `?` below drops it and the caller — which reports the partial summary
    // — would see nothing.
    //
    // The hint reaches both the send and revert arms, which matters here:
    // `registerNode`'s failure modes are pre-flight gas-estimate reverts, so it
    // is the SEND arm that fires in practice.
    let tx = chain_ctx::send(
        bond.registerNode(
            node_id,
            Bytes::from(packed_multiaddrs),
            region.to_string(),
            terms_hash,
            Bytes::from(ed25519_sig),
        ),
        "registerNode",
        Some(
            "most likely the bond does not cover minBond / the declared-capacity curve, the \
             nodeId/address is already bound, or the ownership signature was rejected",
        ),
        tx_slot,
    )
    .await?;
    outcome.tx = Some(tx);
    Ok(outcome)
}

/// Write the outcome as JSON or grep-friendly `key=value` lines. `tx` is
/// `Some` after a successful submit, `None` for a dry run. Pure (`&mut impl
/// Write`) so the output shape is unit-testable without a chain.
pub(crate) fn write_outcome(
    w: &mut impl io::Write,
    o: &RegisterOutcome,
    json: bool,
) -> io::Result<()> {
    if json {
        let value = serde_json::json!({
            "submitted": o.tx.is_some(),
            "tx": o.tx.map(|h| format!("{h:#x}")),
            "node_id": format!("{:#x}", o.node_id),
            "operator": format!("{:#x}", o.operator),
            "chain_id": o.chain_id,
            "capacity_bond": format!("{:#x}", o.capacity_bond),
            "region": o.region,
            "registration_nonce": o.registration_nonce,
            "multiaddrs": o.multiaddr_count,
            "ed25519_sig": format!("0x{}", alloy::hex::encode(&o.ed25519_sig)),
        });
        return writeln!(w, "{value}");
    }
    writeln!(w, "node_id={:#x}", o.node_id)?;
    writeln!(w, "operator={:#x}", o.operator)?;
    writeln!(w, "chain_id={}", o.chain_id)?;
    writeln!(w, "capacity_bond={:#x}", o.capacity_bond)?;
    writeln!(w, "region={}", o.region)?;
    writeln!(w, "registration_nonce={}", o.registration_nonce)?;
    writeln!(w, "multiaddrs={}", o.multiaddr_count)?;
    writeln!(w, "ed25519_sig=0x{}", alloy::hex::encode(&o.ed25519_sig))?;
    match o.tx {
        Some(h) => writeln!(w, "submitted=true tx={h:#x}"),
        None => writeln!(w, "submitted=false dry_run=true"),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
