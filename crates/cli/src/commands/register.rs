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
//! is not already posted (run `decdn node bond` first). `--dry-run` builds
//! and prints everything (it still reads the chain for the nonces the
//! signatures depend on) but does not submit.
//!
//! The signing + submit path is factored into `submit_registration` so
//! `decdn setup` (#933) can drive Step 2.3 with the same already-loaded
//! signer + provider it used for bonding, decrypting the keystore once.

use std::io;
use std::path::Path;

use alloy::primitives::{Address, B256, Bytes};
use alloy::providers::Provider;
use alloy::signers::SignerSync;
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use decdn_common::cli;
use decdn_common::identity;
use decdn_incentive::capacity_bond::CapacityBond;
use decdn_incentive::{bind_sig, node_register};

use crate::commands::{chain_ctx, terms};

/// Entry point for `decdn node register`.
pub async fn run(args: &cli::RegisterArgs, global_config: Option<&Path>) -> anyhow::Result<()> {
    let config_path = args.chain.common.config.as_deref().or(global_config);
    let file = chain_ctx::load_optional_config(config_path)?;
    let resolved = chain_ctx::resolve(&args.chain, &file)?;
    let cb_addr = resolved.capacity_bond_address;

    let signer = chain_ctx::load_operator_signer(&args.chain, &resolved.keystore).await?;
    let provider = decdn_client_pull::provider::build_provider(&resolved.rpc_url, &signer)?;

    // ADR 019 § Terms Acceptance — read the network's current terms hash, then
    // require the operator to accept the matching embedded terms before we sign.
    // Enforced on `--dry-run` too: a dry run still produces a real, submit-able
    // signature that commits to `termsHash`, so it must not be generated over
    // terms the operator hasn't seen / a stale client's hash.
    let terms_hash = CapacityBond::new(cb_addr, &provider)
        .currentTermsHash()
        .call()
        .await
        .with_context(|| {
            format!("failed to read currentTermsHash from CapacityBond at {cb_addr}")
        })?;
    terms::ensure_accepted_async(terms_hash, args.accept_terms).await?;

    let outcome = submit_registration(
        &provider,
        &signer,
        &resolved.data_dir,
        cb_addr,
        resolved.chain_id,
        &args.region,
        &args.multiaddrs,
        terms_hash,
        args.chain.common.dry_run,
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

/// Registration parameters + result, owned so the formatter and `decdn setup`
/// can read them after the signer/provider go out of scope. `tx` is `Some`
/// after a successful submit, `None` for a dry run.
pub(crate) struct RegisterOutcome {
    pub(crate) node_id: B256,
    pub(crate) operator: Address,
    pub(crate) chain_id: u64,
    pub(crate) capacity_bond: Address,
    pub(crate) region: String,
    pub(crate) binding_nonce: u64,
    pub(crate) registration_nonce: u64,
    pub(crate) multiaddr_count: usize,
    pub(crate) binding_sig: Vec<u8>,
    pub(crate) ed25519_sig: Vec<u8>,
    pub(crate) tx: Option<B256>,
}

/// Build the binding + ownership signatures and submit `registerNode`
/// (ADR 019 § Step 2.3) against an already-built `provider` and `signer`.
/// `dry_run` reads the chain for the nonces the signatures depend on but does
/// not submit. Shared by `run` and `decdn setup` so the keystore is decrypted
/// once across bond + register.
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

    // ADR 019 § Terms Acceptance — `terms_hash` is the accepted (and staleness-
    // checked) `currentTermsHash` the caller obtained via `terms::ensure_accepted`
    // before we sign. The binding signature commits to it, and the on-chain
    // contract enforces `termsHash == currentTermsHash`.

    // EIP-712 RegisterNode signature (Ethereum key). `sign_hash_sync` yields a
    // low-s, 27/28-`v` 65-byte signature accepted by the on-chain OZ
    // `SignatureChecker` (same path as `decdn_incentive::bind_sig`).
    let domain = bind_sig::bind_node_id_domain(chain_id, cb_addr);
    let bind_hash =
        bind_sig::register_node_signing_hash(node_id, binding_nonce, terms_hash, &domain);
    let binding_sig = signer.sign_hash_sync(&bind_hash)?.as_bytes().to_vec();

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
        binding_nonce,
        registration_nonce,
        multiaddr_count: multiaddrs.len(),
        binding_sig: binding_sig.clone(),
        ed25519_sig: ed25519_sig.clone(),
        tx: None,
    };

    if dry_run {
        return Ok(outcome);
    }

    let pending = bond
        .registerNode(
            node_id,
            Bytes::from(packed_multiaddrs),
            region.to_string(),
            terms_hash,
            Bytes::from(binding_sig),
            Bytes::from(ed25519_sig),
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
            "binding_nonce": o.binding_nonce,
            "registration_nonce": o.registration_nonce,
            "multiaddrs": o.multiaddr_count,
            "binding_sig": format!("0x{}", alloy::hex::encode(&o.binding_sig)),
            "ed25519_sig": format!("0x{}", alloy::hex::encode(&o.ed25519_sig)),
        });
        return writeln!(w, "{value}");
    }
    writeln!(w, "node_id={:#x}", o.node_id)?;
    writeln!(w, "operator={:#x}", o.operator)?;
    writeln!(w, "chain_id={}", o.chain_id)?;
    writeln!(w, "capacity_bond={:#x}", o.capacity_bond)?;
    writeln!(w, "region={}", o.region)?;
    writeln!(w, "binding_nonce={}", o.binding_nonce)?;
    writeln!(w, "registration_nonce={}", o.registration_nonce)?;
    writeln!(w, "multiaddrs={}", o.multiaddr_count)?;
    writeln!(w, "binding_sig=0x{}", alloy::hex::encode(&o.binding_sig))?;
    writeln!(w, "ed25519_sig=0x{}", alloy::hex::encode(&o.ed25519_sig))?;
    match o.tx {
        Some(h) => writeln!(w, "submitted=true tx={h:#x}"),
        None => writeln!(w, "submitted=false dry_run=true"),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn sample_outcome(tx: Option<B256>) -> RegisterOutcome {
        RegisterOutcome {
            node_id: B256::repeat_byte(0xAB),
            operator: Address::repeat_byte(0xCD),
            chain_id: 31337,
            capacity_bond: Address::repeat_byte(0x01),
            region: "DE".to_string(),
            binding_nonce: 0,
            registration_nonce: 0,
            multiaddr_count: 1,
            binding_sig: vec![0x11; 65],
            ed25519_sig: vec![0x22; 64],
            tx,
        }
    }

    #[test]
    fn dry_run_output_has_signatures() {
        let o = sample_outcome(None);
        let mut buf = Vec::new();
        write_outcome(&mut buf, &o, false).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("submitted=false dry_run=true"), "{s}");
        assert!(s.contains("ed25519_sig=0x2222"), "{s}");
        assert!(s.contains("region=DE"), "{s}");
    }

    #[test]
    fn submitted_output_has_tx() {
        let o = sample_outcome(Some(B256::repeat_byte(0x55)));
        let mut buf = Vec::new();
        write_outcome(&mut buf, &o, false).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("submitted=true tx=0x5555"), "{s}");
    }
}
